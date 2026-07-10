# M3 spine — code dump for transcription (filtered reads)

Supersedes the earlier autocopy/bindings/relay dump on this path. That design
is dead: no `cb_stream_bindings`, no relays, no relay ticker job, no
destination kinds. Why and how the direction changed is recorded in
`spine-usage-sketch.md` — short version: a consumer is a filter plus a
position over a log, and queue retry semantics apply *in place* once queues
take a server-side filter. Topics are the only filter dimension; payload
filtering means promoting the field into the topic.

Settled decisions:

- Filters are **birth policy** on queues and cursors, not read arguments —
  competing consumers of one queue must agree (same reasoning as
  `claim_batch_size`). Stored as the pattern verbatim plus a regex compiled
  once at ensure. Changing a filter later is the ops layer: raw `UPDATE`
  setting both `filter` and `filter_regex = _cb_stream_compile_filter(...)`.
- Grammar unchanged: `*` one segment, `#` zero or more trailing segments,
  final position only. One matcher implementation, in SQL; the Go trie stays
  app-side (in-process dispatchers).
- A `NULL` topic never matches a filter.
- Retry queues never carry a filter: `sr.*` holds only this queue's own
  failures, pre-filtered by construction.
- `claim_batch_size` counts **positions**, not matches. A sparse filter
  closes near-empty claims fast; cursor reads may return zero rows while
  still advancing.
- Edits go **in place** into `stream/migrations/00001_stream.sql` (schema is
  pre-release; same precedent as the D27/D28 rework). No version bump.
- No engine-created content indexes. Read SQL stays index-usable for later,
  but the prefix column and the claim fast-forward optimization wait for a
  deep-sparse-replay customer.

Build order: (1) compiler + columns + ensure params, (2) read paths
(`cb_stream_read`, new `cb_stream_read_claim`, quarantine predicate),
(3) Go opts + fetch swap, (4) `cb_stream_publish_messages` + Go
`PublishMessages`, (5) tests, (6) doc fixes.

---

## 1. Columns and constraint (table DDL)

`cb_stream_cursors` gains:

```sql
    filter text,       -- topic filter pattern; NULL reads everything
    filter_regex text  -- compiled by _cb_stream_compile_filter at ensure
```

`cb_stream_queues` gains the same two columns, plus a named constraint next
to `cb_stream_queues_retry_batch_size`:

```sql
    CONSTRAINT cb_stream_queues_retry_no_filter
        CHECK (left(stream, 3) <> 'sr.' OR filter IS NULL)
```

---

## 2. `_cb_stream_compile_filter`

Port of the old `cb_bind` validator + regex builder (migrations/
00001_catbird.sql:384-462), reduced to one function that returns the
compiled regex or raises. Place it near `cb_valid_name`.

```sql
-- +goose statementbegin
-- Validate a topic filter and compile it to a regex. '*' matches one
-- segment, '#' matches zero or more trailing segments and must be the
-- final segment. Raises for anything else.
CREATE FUNCTION _cb_stream_compile_filter(pattern text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    _tokens text[];
    _token text;
    _i int;
    _n int;
    _regex text;
BEGIN
    IF _cb_stream_compile_filter.pattern IS NULL OR _cb_stream_compile_filter.pattern = '' THEN
        RAISE EXCEPTION 'catbird: filter cannot be empty' USING ERRCODE = 'IRD01';
    END IF;

    IF _cb_stream_compile_filter.pattern !~ '^[a-zA-Z0-9._#*-]+$' THEN
        RAISE EXCEPTION 'catbird: filter % may only contain a-z, A-Z, 0-9, ., _, -, * and #',
            _cb_stream_compile_filter.pattern USING ERRCODE = 'IRD01';
    END IF;

    IF _cb_stream_compile_filter.pattern ~ '\.\.'
    OR _cb_stream_compile_filter.pattern ~ '(^\.|\.$)' THEN
        RAISE EXCEPTION 'catbird: filter % cannot contain empty segments',
            _cb_stream_compile_filter.pattern USING ERRCODE = 'IRD01';
    END IF;

    _tokens := string_to_array(_cb_stream_compile_filter.pattern, '.');
    _n := array_length(_tokens, 1);
    FOR _i IN 1.._n LOOP
        _token := _tokens[_i];
        IF _token = '#' AND _i <> _n THEN
            RAISE EXCEPTION 'catbird: # must be the final segment of filter %',
                _cb_stream_compile_filter.pattern USING ERRCODE = 'IRD01';
        END IF;
        IF _token <> '*' AND _token <> '#' AND _token ~ '[*#]' THEN
            RAISE EXCEPTION 'catbird: * and # must be whole segments in filter %',
                _cb_stream_compile_filter.pattern USING ERRCODE = 'IRD01';
        END IF;
    END LOOP;

    IF _cb_stream_compile_filter.pattern = '#' THEN
        RETURN '^[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)*$';
    END IF;

    -- Strip a trailing '.#' (its zero-or-more tail is appended below),
    -- escape literal dots, then widen '*' to one-segment matches.
    _regex := regexp_replace(_cb_stream_compile_filter.pattern, '\.#$', '');
    _regex := regexp_replace(_regex, '\.', '\\.', 'g');
    _regex := regexp_replace(_regex, '\*', '[a-zA-Z0-9_-]+', 'g');

    IF _cb_stream_compile_filter.pattern ~ '\.#$' THEN
        -- 'a.#' also matches the bare 'a': the tail may be zero segments
        RETURN '^' || _regex || '(\.[a-zA-Z0-9_-]+)*$';
    END IF;
    RETURN '^' || _regex || '$';
END; $$;
-- +goose statementend
```

---

## 3. `cb_stream_ensure_cursor` — filter param

Signature gains `filter text DEFAULT NULL`; the insert stores pattern and
compiled regex. Birth-only like everything else in the function.

```sql
CREATE FUNCTION cb_stream_ensure_cursor(stream text, cursor text, start_pos bigint DEFAULT NULL, filter text DEFAULT NULL)
```

```sql
DECLARE
    _start bigint;
    _regex text;
```

after the stream check:

```sql
    IF cb_stream_ensure_cursor.filter IS NOT NULL THEN
        _regex := _cb_stream_compile_filter(cb_stream_ensure_cursor.filter);
    END IF;

    INSERT INTO cb_stream_cursors (stream, name, pos, filter, filter_regex)
    VALUES (cb_stream_ensure_cursor.stream, cb_stream_ensure_cursor.cursor, _start,
            cb_stream_ensure_cursor.filter, _regex)
    ON CONFLICT ON CONSTRAINT cb_stream_cursors_pkey DO NOTHING;
```

---

## 4. `cb_stream_ensure_queue` — filter param

Signature gains `filter text DEFAULT NULL` (last param; the Go client calls
with named args). Compile once, store on the **base** queue row only:

```sql
DECLARE
    _start bigint;
    _regex text;
```

```sql
    IF cb_stream_ensure_queue.filter IS NOT NULL THEN
        _regex := _cb_stream_compile_filter(cb_stream_ensure_queue.filter);
    END IF;
```

The base-row `INSERT` column list gains `filter, filter_regex` with values
`cb_stream_ensure_queue.filter, _regex`. The retry-row `INSERT..SELECT` is
**unchanged** — it does not copy the filter columns, so `sr.*` queues get
NULL, which the `cb_stream_queues_retry_no_filter` constraint pins.

---

## 5. `cb_stream_read` — honor the cursor's filter

The cursor advances over the whole scanned range; only matches return. The
batch-bounding subquery stays unfiltered — `batch_size` counts scanned rows,
so a filtered read may return fewer rows than `batch_size`, or none, while
still advancing.

```sql
DECLARE
    _pos bigint;
    _regex text;
    _new_pos bigint;
BEGIN
    SELECT c.pos, c.filter_regex INTO _pos, _regex FROM cb_stream_cursors c
    WHERE c.stream = cb_stream_read.stream AND c.name = cb_stream_read.cursor
    FOR UPDATE;
```

and in the RETURN QUERY:

```sql
    RETURN QUERY
    SELECT m.* FROM cb_stream_messages m
    WHERE m.stream = cb_stream_read.stream
      AND m.pos > _pos AND m.pos <= _new_pos
      AND (_regex IS NULL OR m.topic ~ _regex) -- a NULL topic never matches
    ORDER BY m.pos;
```

Everything else (max-pos subquery, cursor UPDATE) is unchanged.

---

## 6. `cb_stream_read_claim` — claimed-range fetch moves into SQL

Replaces the inline `SELECT ... WHERE pos BETWEEN` in `consume_queue.go`, so
every client gets the queue's filter without knowing it exists. Positions in
the range that don't match were never the queue's to handle; closing the
claim still advances over them. (An undefined queue returns no rows rather
than raising — the caller got the range from `cb_stream_claim`, which
already raised.)

```sql
-- +goose statementbegin
-- The messages of a claimed range, in order, honoring the queue's filter.
CREATE FUNCTION cb_stream_read_claim(stream text, queue text, from_pos bigint, to_pos bigint)
RETURNS SETOF cb_stream_messages
LANGUAGE sql AS $$
    SELECT m.*
    FROM cb_stream_messages m
    JOIN cb_stream_queues q
      ON q.stream = cb_stream_read_claim.stream
     AND q.name   = cb_stream_read_claim.queue
    WHERE m.stream = cb_stream_read_claim.stream
      AND m.pos BETWEEN cb_stream_read_claim.from_pos AND cb_stream_read_claim.to_pos
      AND (q.filter_regex IS NULL OR m.topic ~ q.filter_regex)
    ORDER BY m.pos;
$$;
-- +goose statementend
```

---

## 7. `_cb_stream_quarantine` — the filter bug fix

The quarantine loop republishes **every** message in the claimed range to
`sr.*`. On a filtered queue that would quarantine messages the queue never
delivered. `_q` (the base queue row) is already fetched for policy; the loop
gains the predicate:

```sql
    FOR _m IN
        SELECT m.* FROM cb_stream_messages m
        WHERE m.stream = _stream AND m.pos BETWEEN _from_pos AND _to_pos
          AND (_q.filter_regex IS NULL OR m.topic ~ _q.filter_regex)
        ORDER BY m.pos
    LOOP
```

(Harmless on retry-stream claims: `sr.*` messages carried their topics from
matches, so they all pass the base filter.)

`cb_stream_fail` needs no change: it is per-message, and a consumer can only
fail a message it received — which the filtered fetch already selected.

---

## 8. `cb_stream_publish_messages` — per-message topics in batch append

Requirement 2 from the usage sketch: one `Revise` emits many events with
different topics; `cb_stream_publish_payloads` takes a single topic.
Envelope form, minimal version: a validated loop over the public
single-publish path, which keeps the `cb_` header guard, key dedup, and
notify semantics without duplicating them. At bibliographic-edit batch sizes
the loop is fine; the set-based temp-table design (sketched pre-Go-phase for
bulk-import volume) stays the upgrade path if a profiler ever asks.

```sql
-- +goose statementbegin
-- Batch publish with per-message topics, headers and keys. messages is a
-- jsonb array of {payload, topic?, headers?, key?} envelopes. Returns one
-- row per element, in input order.
CREATE FUNCTION cb_stream_publish_messages(stream text, messages jsonb)
RETURNS TABLE (ref_kind cb_ref_kind, ref_id bigint, existing boolean)
LANGUAGE plpgsql AS $$
DECLARE
    _m jsonb;
BEGIN
    IF cb_stream_publish_messages.messages IS NULL
    OR jsonb_typeof(cb_stream_publish_messages.messages) <> 'array' THEN
        RAISE EXCEPTION 'catbird: messages must be a JSON array' USING ERRCODE = 'IRD01';
    END IF;

    FOR _m IN SELECT e.* FROM jsonb_array_elements(cb_stream_publish_messages.messages) e LOOP
        IF _m->'payload' IS NULL THEN
            RAISE EXCEPTION 'catbird: message without payload' USING ERRCODE = 'IRD01';
        END IF;

        RETURN QUERY
        SELECT p.ref_kind, p.ref_id, p.existing
        FROM cb_stream_publish(
            cb_stream_publish_messages.stream,
            _m->>'topic',
            _m->'payload',
            coalesce(_m->'headers', '{}'),
            _m->>'key') p;
    END LOOP;
END; $$;
-- +goose statementend
```

---

## 9. Down section additions

```sql
DROP FUNCTION cb_stream_publish_messages(text, jsonb);
DROP FUNCTION cb_stream_read_claim(text, text, bigint, bigint);
DROP FUNCTION _cb_stream_compile_filter(text);
```

`cb_stream_ensure_cursor`/`cb_stream_ensure_queue` drops need their new
signatures. The columns and constraint go down with their tables.

---

## 10. Go changes

`stream/cursor.go`:

```go
type CursorOpts struct {
	// StartPos: where a new cursor begins ... (unchanged)
	StartPos *int64
	// Filter: topic filter, applied server-side. '*' matches one segment,
	// '#' zero or more trailing segments. "" reads everything. The cursor
	// advances over everything it scans, so a filtered read can return
	// fewer messages than the batch size, or none.
	Filter string
}
```

`EnsureCursor` passes `nullText(o.Filter)` as `$4`.

`stream/queue.go`: `QueueOpts` gains the same `Filter string` field (same
doc comment, plus: fixed at the queue's creation; all consumers share it);
`EnsureQueue` adds `filter => $N` to its named-args call.

`stream/consume_queue.go`: the claimed-range fetch becomes

```go
	rows, err := pool.Query(ctx, `
		SELECT m.id, m.stream, m.pos, coalesce(m.topic, ''), m.payload, m.headers, m.created_at
		FROM cb_stream_read_claim($1, $2, $3, $4) m`,
		stream, *fromPos, *toPos)
```

(with the queue name — `$1..$4` = stream, queue, from, to).

`stream/publish.go`:

```go
// BatchMessage is one element of PublishMessages.
type BatchMessage struct {
	Topic   string
	Payload any
	Headers map[string]any // cb_ keys are reserved
	Key     string         // deduplication key: keep-oldest
}

func PublishMessages(ctx context.Context, conn Conn, stream string, msgs []BatchMessage) ([]Ref, error)
```

marshals to one jsonb array and scans `(ref_kind, ref_id, existing)` per
element from `cb_stream_publish_messages($1, $2)`.

---

## 11. Tests (`stream/stream_test.go`) — the M3 exit gate

1. **TestCompileFilter** — grammar table ported from `bindings_test.go`,
   driven through SQL: `SELECT $topic ~ _cb_stream_compile_filter($pattern)`
   for the match cases, error cases assert IRD01 (`ErrInvalid`). Include:
   zero-token tail (`a.#` matches `a`), bare `#`, mid-pattern `*`, rejected
   `#` not final, rejected in-token wildcards, rejected empty segments.
2. **TestFilteredQueue** — the ORCID shape: publish mixed topics, a queue
   with `Filter: "record.work.#"` and `StartPos: At(0)`; consumers see only
   matches; `closed_pos` advances over non-matches; a failed match lands in
   `sr.*` and redelivers; **quarantine leaks nothing**: crash a claim whose
   range holds matching and non-matching messages, assert only matching
   copies appear in `sr.*`.
3. **TestFilteredCursor** — filtered `Read` returns matches only, advances
   over a batch with zero matches, and never returns NULL-topic messages;
   an unfiltered cursor on the same stream sees everything (independence).
4. **TestPublishMessages** — per-message topics land, headers/keys per
   element work, key dedup returns `existing`, `cb_` header rejected,
   element without payload rejected, refs come back in input order.
5. **Replay** — late `EnsureQueue` with `StartPos: At(0)` + filter
   materializes retained history into handler calls (the regression
   incident's mechanical answer).

---

## 12. Doc fixes (do as part of the work)

- `docs/plan/02-spine.md`: rewrite around filtered consumers + the wire
  path; the bindings/relay chapter goes. The usage sketch's "collapse"
  section is the outline.
- `docs/plan/README.md`: new decision-log entry (read filters replace
  routing; supersedes D8's fan-out framing — there is no fan-out).
- `docs/plan/05-milestones.md`: M3 line becomes filtered reads + batch
  envelopes.
- `CLAUDE.md`: the "What is Catbird?" wildcard line still says `?`/`*` —
  correct to `*` one segment / `#` trailing; drop "topic routing via
  bindings" phrasing when the old surface goes.
- `docs/plan/01-stream.md`: §4 (cursor read) and §5 (claims) gain the
  filter semantics — scanned-range advancement, positions-not-matches
  batch sizing, retry queues unfiltered by construction.
