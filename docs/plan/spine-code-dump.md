# M3 spine — code dump for transcription

Settled decisions: two tables (relay owns cursor, bindings are the pattern list);
relay runs as a ticker job; wildcard grammar is `*` (one token) / `#` (tail) per
`topic_trie.go`; Unbind drops the pattern only, relay + cursor persist.

Deferred out of M3: the `catbird.Publish`/`catbird.Bind` root facade (M6, import
cycle) and enriching the NOTIFY payload beyond topic-only (M5, wire's call).

Build order: (1) migration, (2) port the trie + `Bind`/`Unbind`, (3) relay kind +
runner + `RunJobs` wiring, (4) tests.

---

## 1. `stream/migrations/00002_spine.sql`

```sql
-- +goose up

-- The root stream: one insert per Publish, bindings evaluated at read time.
-- 7-day age cap = relay lag + replay window (docs/plan/02-spine.md §1).
SELECT _cb_stream_ensure('bus', interval '7 days');

-- Names each relay's cursor; a sequence keeps them short and unique so they fit
-- the single-segment, <=20-byte cursor-name rule.
CREATE SEQUENCE cb_stream_relay_cursor_seq;

-- One relay = one cursor over the source stream, per destination. The first
-- Bind to a destination creates this row and its cursor.
CREATE TABLE cb_stream_relays (
    stream           text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    destination_kind text NOT NULL CHECK (cb_valid_name(destination_kind)), -- registered Go-side, not an enum
    destination      text NOT NULL,
    cursor           text NOT NULL, -- the cb_stream_cursors name this relay advances
    PRIMARY KEY (stream, destination_kind, destination)
);

-- The pattern list feeding a relay's matcher. Adding or removing a pattern never
-- touches the cursor.
CREATE TABLE cb_stream_bindings (
    stream           text NOT NULL,
    destination_kind text NOT NULL,
    destination      text NOT NULL,
    pattern          text NOT NULL, -- topic_trie grammar: '*' one token, '#' tail
    identity_from    text,          -- inbox kind only (04); NULL otherwise
    PRIMARY KEY (stream, destination_kind, destination, pattern),
    FOREIGN KEY (stream, destination_kind, destination)
        REFERENCES cb_stream_relays(stream, destination_kind, destination) ON DELETE CASCADE
);

-- +goose statementbegin
-- Route a topic pattern on a source stream to a destination. The first bind to a
-- (stream, kind, destination) creates that destination's relay and its cursor;
-- only that call's start_pos is honored. Idempotent: an identical bind writes
-- nothing.
CREATE FUNCTION cb_stream_bind(
    stream           text,
    destination_kind text,
    destination      text,
    pattern          text,
    start_pos        bigint DEFAULT NULL,
    identity_from    text   DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _cursor text;
BEGIN
    IF NOT cb_valid_name(cb_stream_bind.destination_kind) THEN
        RAISE EXCEPTION 'catbird: invalid destination kind %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_bind.destination_kind USING ERRCODE = 'IRD01';
    END IF;

    PERFORM 1 FROM cb_streams s WHERE s.name = cb_stream_bind.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', cb_stream_bind.stream USING ERRCODE = 'IRD02';
    END IF;

    -- One relay (one cursor) per destination. The first bind creates it; later
    -- binds to the same destination inherit the existing cursor, so start_pos can
    -- replay history only once.
    INSERT INTO cb_stream_relays AS r (stream, destination_kind, destination, cursor)
    VALUES (cb_stream_bind.stream, cb_stream_bind.destination_kind, cb_stream_bind.destination,
            'relay_' || nextval('cb_stream_relay_cursor_seq'))
    ON CONFLICT ON CONSTRAINT cb_stream_relays_pkey DO NOTHING
    RETURNING r.cursor INTO _cursor;

    IF _cursor IS NOT NULL THEN
        PERFORM cb_stream_ensure_cursor(cb_stream_bind.stream, _cursor, cb_stream_bind.start_pos);
    END IF;

    INSERT INTO cb_stream_bindings AS b
        (stream, destination_kind, destination, pattern, identity_from)
    VALUES (cb_stream_bind.stream, cb_stream_bind.destination_kind,
            cb_stream_bind.destination, cb_stream_bind.pattern, cb_stream_bind.identity_from)
    ON CONFLICT ON CONSTRAINT cb_stream_bindings_pkey DO NOTHING;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Remove one pattern from a destination's routing. The relay and its cursor stay:
-- position is preserved and a relay with no patterns is simply skipped by the
-- runner. Returns whether a binding was removed.
CREATE FUNCTION cb_stream_unbind(
    stream           text,
    destination_kind text,
    destination      text,
    pattern          text
)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _count int;
BEGIN
    DELETE FROM cb_stream_bindings b
    WHERE b.stream           = cb_stream_unbind.stream
      AND b.destination_kind = cb_stream_unbind.destination_kind
      AND b.destination      = cb_stream_unbind.destination
      AND b.pattern          = cb_stream_unbind.pattern;
    GET DIAGNOSTICS _count = ROW_COUNT;
    RETURN _count > 0;
END; $$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_stream_unbind(text, text, text, text);
DROP FUNCTION cb_stream_bind(text, text, text, text, bigint, text);
DROP TABLE cb_stream_bindings;
DROP TABLE cb_stream_relays;
DROP SEQUENCE cb_stream_relay_cursor_seq;
DELETE FROM cb_streams WHERE name = 'bus'; -- cascades to its cursors/messages rows
DROP TABLE IF EXISTS cbm__bus;             -- the list partition _cb_stream_ensure created
```

> After adding this file, bump `SchemaVersion` in `stream/migrate.go` to 2.

---

## 2. Port the topic matcher into the stream package

Copy `topic_trie.go` and `topic.go` from the repo root into `stream/`, changing
only the package line (`package catbird` → `package stream`). No logic changes —
the reuse map says port as-is. `topic_trie.go` powers the relay matcher;
`topic.go` (`matchTopic`) comes along for the bindings tests.

```
cp topic_trie.go stream/topic_trie.go   # then edit: package stream
cp topic.go      stream/topic.go        # then edit: package stream
```

Grammar reminder (the code's, which now wins): `*` = one token, `#` = zero or more
trailing tokens. The `?`/`*` phrasing in `docs/plan/02-spine.md §3` and `CLAUDE.md`
is stale — fix it (see §6).

---

## 3. `stream/bind.go`

```go
package stream

import "context"

const busStream = "bus"

// BindOpts tunes a binding. Zero fields mean the defaults.
type BindOpts struct {
	Stream       string // source root stream; "" means "bus"
	StartPos     *int64 // honored only when this bind creates the relay's cursor
	IdentityFrom string // inbox kind (04); "" otherwise
}

// Bind routes topics matching pattern on the source stream to a destination of
// the given kind. The first bind to a (stream, kind, destination) creates that
// destination's relay cursor; only that call's StartPos is honored. Idempotent.
func Bind(ctx context.Context, conn Conn, kind, destination, pattern string, opts ...BindOpts) error {
	var o BindOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	stream := o.Stream
	if stream == "" {
		stream = busStream
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_stream_bind($1, $2, $3, $4, $5, $6)`,
		stream, kind, destination, pattern, o.StartPos, nullText(o.IdentityFrom))
	return wrapErr(err)
}

// Unbind removes one pattern from a destination's routing. The relay and its
// cursor stay in place. Reports whether a binding was removed.
func Unbind(ctx context.Context, conn Conn, kind, destination, pattern string, opts ...BindOpts) (bool, error) {
	var o BindOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	stream := o.Stream
	if stream == "" {
		stream = busStream
	}
	var deleted bool
	err := conn.QueryRow(ctx,
		`SELECT cb_stream_unbind($1, $2, $3, $4)`,
		stream, kind, destination, pattern).Scan(&deleted)
	return deleted, wrapErr(err)
}
```

---

## 4. `stream/relay.go`

```go
package stream

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const relayBatchSize = 100

// RelayWriter materializes one matched message into a destination, inside the
// relay's transaction. The destination write and the cursor advance commit
// together, so the relay never duplicates into its destination.
type RelayWriter func(ctx context.Context, tx pgx.Tx, destination string, m Message) error

// relayKinds is the process-wide registry. The stream kind registers below;
// flow and wire register theirs at import (M4/M5). A kind not registered in this
// process means its relays are run by a node that did import it.
var relayKinds = map[string]RelayWriter{}

// RegisterRelayKind registers a destination kind's writer. Call it from an init
// function so importing the package is enough to make the kind runnable.
func RegisterRelayKind(kind string, w RelayWriter) { relayKinds[kind] = w }

func init() {
	// The stream kind forwards the message into another stream, unchanged.
	RegisterRelayKind("stream", func(ctx context.Context, tx pgx.Tx, dest string, m Message) error {
		_, err := tx.Exec(ctx,
			`SELECT _cb_stream_publish($1, $2, $3, $4)`,
			dest, m.Topic, m.Payload, m.Headers)
		return wrapErr(err)
	})
}

// relayDef is one relay plus the patterns feeding it. Column order matches the
// query in relay() for RowToStructByPos.
type relayDef struct {
	Stream      string
	Kind        string
	Destination string
	Cursor      string
	Patterns    []string
}

// relay runs one pass over every relay that has at least one binding: read a
// batch, forward the matches, advance the cursor — all in one transaction per
// relay, which is exactly-once materialization. The trie is rebuilt each pass,
// so binding changes take effect from the next batch.
func relay(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	rows, err := pool.Query(ctx, `
		SELECT r.stream, r.destination_kind, r.destination, r.cursor,
		       array_agg(b.pattern) AS patterns
		FROM cb_stream_relays r
		JOIN cb_stream_bindings b
		  ON b.stream           = r.stream
		 AND b.destination_kind = r.destination_kind
		 AND b.destination      = r.destination
		GROUP BY r.stream, r.destination_kind, r.destination, r.cursor`)
	if err != nil {
		return 0, err
	}
	relays, err := pgx.CollectRows(rows, pgx.RowToStructByPos[relayDef])
	if err != nil {
		return 0, err
	}

	n := 0
	for _, r := range relays {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		w, ok := relayKinds[r.Kind]
		if !ok {
			continue // kind not registered here; another node runs it
		}
		match := newPatternSet(r.Patterns)

		wrote := 0
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			// cb_stream_read takes FOR UPDATE on the cursor row: one worker
			// advances a given relay at a time.
			batch, err := Read(ctx, tx, r.Stream, r.Cursor, relayBatchSize)
			if err != nil || len(batch) == 0 {
				return err
			}
			for _, m := range batch {
				if !match.has(m.Topic) {
					continue
				}
				if err := w(ctx, tx, r.Destination, m); err != nil {
					return err
				}
				wrote++
			}
			return nil
			// commit: destination writes + cursor advance land together.
		})
		if err != nil {
			continue // one relay's failure must not stall the others; retry next tick
		}
		n += wrote
	}
	return n, nil
}

// patternSet answers "does this topic match any of the relay's patterns" using
// the ported trie.
type patternSet struct{ t *topicTrie[struct{}] }

func newPatternSet(patterns []string) patternSet {
	t := newTopicTrie[struct{}]()
	for _, p := range patterns {
		t.add(p, struct{}{})
	}
	return patternSet{t}
}

func (p patternSet) has(topic string) bool { return len(p.t.match(topic, nil)) > 0 }
```

---

## 5. Wire the relay job into `RunJobs` (`stream/jobs.go`)

Add one field to `JobsOpts`:

```go
	RelayInterval time.Duration // 100ms: relay lag (bus -> destinations)
```

Default it alongside the others in `RunJobs`:

```go
	if o.RelayInterval <= 0 {
		o.RelayInterval = 100 * time.Millisecond
	}
```

Register the job next to `stream.assign` / `stream.deliver` / `stream.prune`:

```go
	t.Add(ticker.Job{Name: "stream.relay", Every: o.RelayInterval,
		Run: func(ctx context.Context) (int, error) { return relay(ctx, pool) }})
```

> Relay lag is assign-tick + relay-tick: a bus message needs a position before an
> ordered read returns it. That's the poll-only prediction; NOTIFY tightens it at M5.

---

## 6. Doc fixes (do as part of the work)

- `docs/plan/02-spine.md §3`: change "today's grammar (`?` single token, `*` tail)"
  to `*` single token / `#` tail, matching the ported `topic_trie.go`.
- `CLAUDE.md` "What is Catbird?" bullet: same correction to the wildcard grammar.

---

## 7. Tests to add (`stream/stream_test.go`) — the M3 exit gate

1. **LiveView** — `Publish` into `bus` inside a tx, roll back; assert the
   destination stream stays empty and no `<schema>.cbs_bus` notification fired.
2. **Relay crash mid-batch → no duplicates** — kill the relay between write and
   commit; the batch re-reads and re-writes cleanly; destination count == matched
   count.
3. **Late binding replays history** — publish to `bus`, then
   `Bind("stream", dest, pattern, BindOpts{StartPos: At(0)})`; assert the relay
   materializes the pre-binding history.
4. **Ported `bindings_test.go` semantics** — the wildcard match cases over the new
   tables.
