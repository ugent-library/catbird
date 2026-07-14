# 02 — filters: topics, conditions, and the end of routing

> Rewritten 2026-07-12. This chapter used to describe the spine: a well-known
> `bus` stream, binding rows, and relay consumers materializing matches into
> destinations. That design was evaluated against both production apps and
> replaced by server-side read filters before anything was built (D29). The
> journey and the grounding evidence are in `spine-usage-sketch.md`; this
> chapter describes what shipped.
>
> Vocabulary updated 2026-07-14 (branch experiment): queue → **subscription**
> (D37), retry and dead-letter state are rows (D35, D36), and dispatched work
> is a run (D34) — the consumer-shape table below reflects it.

## 1. There is no routing (supersedes D8's framing)

The vision drew `Publish` as N inserts, one per bound stream (fan-out-on-write).
The first revision inverted that: one insert, read-side relays (fan-out-on-read,
D8). The shipped design ends the argument by removing its subject: **there is no
fan-out at all**. Topics stop deciding where messages *go* and become metadata
consumers *select on*. Publish is one insert, always, and never knows its
consumers.

What remains is one mental model: **a consumer is a filter plus a position over
a log.** Three durability flavors of the same idea — no position (wire,
ephemeral, in-process match), an ordered position (cursor), claimed ranges with
retries (subscription).

The usage sketch's evidence, compressed: every live enqueue in both apps is
producer-knows-consumer (same-transaction enqueue — a one-step run since D37);
the consumers that want a *subset* of a feed want retry semantics on that
subset, which filters give them in place, with no copy; and every topic-routing
path ever wired in either app died — one by silent regression, one deleted after
a day. Late binding and replay survive untouched: they were always cursor-start
semantics (`StartPos`), never copy semantics.

## 2. The filter: two small languages

A filter is birth policy on a subscription or cursor — like `claim_batch_size`,
competing consumers of one subscription must agree, so it is never a read argument. It
is parsed **once at registration**; reads evaluate precompiled artifacts only.
Two parts, AND-ed, each doing one job:

- **`topic`** — a bare pattern over the topic: `*` matches one segment, `#`
  matches zero or more trailing segments (`record.work.#` also matches
  `record.work`). Compiled to a regex at ensure. The fast path, and the only
  dimension that may ever be index-assisted.
- **`condition`** — an AND-only expression over `$.headers` and `$.payload`:
  nested-key existence and scalar equality in the MVP
  (`exists($.payload.made_public_at)`, `$.payload.type == "work"`). Parsed by a
  whitelist (the parser is the validator — anything unsupported fails loud at
  registration) and disassembled into one generated jsonpath per column.
  Equality is strict across JSON's types and numeric within `number` (JSON has
  one number type; serializers disagree about its spelling). Lax array
  unwrapping is a feature: `$.payload.tags == "urgent"` matches
  `{"tags": ["urgent", "review"]}`. Evaluation can never error at read time —
  an unknown result is a non-match. The grammar grows by whitelist only;
  `like_regex` (a second regex dialect) and `datetime()` (session-timezone
  dependent) never enter. The migration is the naming authority; the accept
  and reject tables live in `TestCompileTopic`/`TestCompileCondition`.

The shared rules:

- A cursor advances over the whole **scanned** range: a filtered read may
  return fewer rows than the batch size, or none, and still advance.
- Claims cover every position in their range, matching or not:
  `claim_batch_size` counts positions, and a sparse filter just closes
  near-empty claims fast. The filter applies in the claimed-range fetch
  (`cb_stream_read_claim`) and in quarantine — non-matches never become
  retry rows.
- Retry rows are never filtered: `cb_stream_retries` holds only its
  subscription's own failures, pre-filtered by construction (D35).
- **Topics select, conditions prune.** Any condition costs a per-row jsonb
  evaluation and is never index-assisted. The engine creates no content
  indexes and correctness never depends on one; a deep sparse replay can add
  an app-owned topic index on the stream's partition — the same contract as
  read-only SQL over a stream.

## 3. Publish

Two functions, one invariant:

```go
// one message — the five-minute call
stream.Publish(ctx, tx, "records", "record.work.updated", ev)
stream.Publish(ctx, tx, "blob_deletes", "", in, stream.PublishOpts{Key: blobID})

// several — PublishMessages ≡ N × Publish, atomically, in one call
stream.PublishMessages(ctx, tx, "records", []stream.BatchMessage{
    {Topic: "record.work.updated", Payload: ev1},
    {Topic: "record.file.created", Payload: ev2, Key: "f-" + id},
})
```

The envelope carries the full option set (topic, headers, key, delay,
deliver_at); refs return in input order. The SQL side takes a single `jsonb`
array — one JSON text any client language produces natively, no
PG-array-literal escaping. `cb_stream_publish_payloads` (batch with one shared
topic) is retired: `PublishMessages` subsumes it, and the set-based fast path
is a deferred optimization with a written design and a trigger (05,
"Deferred optimizations"). The root-package `catbird.Publish` facade still
arrives at M6.

## 4. Consumers by shape

What replaced the destination-kind table — each need maps onto the substrate
directly:

| Need | Shape |
|---|---|
| Projection over the whole feed (search index, representations) | plain cursor |
| Subset with retry/dead-row semantics (ORCID push, per-target deletes) | **filtered subscription on the feed** |
| Producer knows the consumer (blob GC, LDN outbox) | same-tx `cb_flow_run` — a one-step run with a dedup key (D37); a stream-only install composes `Publish` to an own stream + a subscription instead |
| Many dynamic user-defined subscribers (webhooks) | one dispatcher cursor + the Go trie over app rows + one run per delivery (D37) |
| User-facing notification inbox | explicit writes by handlers (04) — identity is data in the handler's hand |

The exactly-once guarantee moves with the shapes: any filtered cursor consumer
whose effects are rows in the same Postgres commits effects and cursor advance
in one transaction — the guarantee the relay row in the README table used to
claim, now without the relay. And a handler that calls `cb_flow_run` creates
work from events under the same guarantee — the composition rule (README,
"Two shapes, one discipline").

The Go trie (`topic_trie.go`) is not dead code: it is the app-side matcher for
the dispatcher shape — one event against many subscriber patterns, in process,
built from app rows. The engine's matcher is the SQL compiler; the trie never
became engine code.

## 5. The ephemeral path (wire)

Unchanged in principle from the original chapter. wire never touches storage.
Every append fires `pg_notify` on a channel named after its stream, inside the
transaction; Postgres delivers on commit, so push-only-on-commit is free. wire
subscribes to the channels of the streams it cares about — domain streams like
`records`, not a well-known bus. The notify follows the *actual* append: a
delayed publish notifies when due (`_cb_stream_deliver_pending`), a
dedup-skipped publish not at all. NOTIFY deduplicates identical (channel,
payload) pairs per transaction, so batch publishes cost one notification per
distinct topic. The kernel's notifier holds the one LISTEN connection per
process and fans out to in-process subscribers (04); it arrives at M5 (D17).
Payloads above the 8000-byte NOTIFY limit send topic-only; wire re-pulls state.

## 6. What died, and what would revive it

Deleted before ever being built: `cb_stream_bindings`, relays, the relay ticker
job, destination kinds, the kind registry, `identity_from`, the well-known
`bus` stream. The autocopy idea — materialize a filtered subset into another
stream — comes back only if a real customer needs what filters cannot do:
fan-in from several streams, transformation in flight, or a copy that outlives
its source's retention. None exists in either app or their roadmaps.

Shipped and tested (M3 exit, 2026-07-12): compilers with grammar tables,
filtered cursors and queues through the real APIs, a live filtered worker end
to end (delivers exactly the matches, failed match returns through `sr.*` —
retry rows after M3r, closed position keeps up over undelivered ranges), and
batch publish with the full envelope matrix.
