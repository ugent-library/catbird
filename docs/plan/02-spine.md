# 02 — spine: publish, topics, bindings, relays

The spine is not a module (D8). It is a **usage pattern of the substrate**: one
well-known root stream, topic filters, and relay consumers. This resolves the
vision's open decisions 1 and 3 by dissolution — there is nothing left to place.

## 1. Fan-out-on-read (D8)

The vision drew `Publish` as N inserts, one per bound stream, inside the caller's
transaction. This plan inverts it: **publish writes exactly one row** into a
topic-keyed root stream (`bus` by default); bindings are evaluated at *read* time.

Why (recap of the design review):

- Publish is O(1) regardless of bindings — the code path inside user request
  transactions gets cheaper, not amplified.
- Late binding becomes real: a new binding can start from history (within the root
  stream's retention), not just the future.
- Per-identity routing stops being binding-table sprawl. The vision's own example
  (`toInbox(id)`, `toWire("user."+id)`) interpolates runtime identities into
  bindings — a sign they were data all along. Under fan-out-on-read they are
  ordinary routing decisions inside a relay handler.
- The LiveView property survives untouched: nothing fires before commit. What
  changes is that queue materialization happens milliseconds *after* commit
  (relay lag) instead of *during* it.

Absent consumers (from the design conversation, recorded here): the durable thing
is the **cursor row**, not the process. Subscribed-but-down consumers resume from
their cursor; not-yet-subscribed consumers can replay within retention — strictly
better than fan-out-on-write, where an unrouted message is gone forever. Long-term
absence is absorbed by destination stores (work streams, the inbox), each with its
own retention; the root stream only needs to retain relay lag plus the desired
replay window. Default root retention: age-cap, 7 days.

## 2. API

```go
// the five-minute API, in the root package
catbird.Publish(ctx, tx, "order.placed", order)          // one insert, atomic with tx
catbird.Publish(ctx, tx, "order.placed", order,
    catbird.WithKey("order-"+id),                         // dedup keep-oldest (01 §8)
    catbird.WithDelay(10*time.Minute),                    // pending (01 §6)
    catbird.WithCoalesce("reindex-"+id, 30*time.Second))  // keep-newest (01 §6)
```

`Publish` = `stream.Append("bus", topic, …)` plus one `pg_notify('cb_wire', topic)`
for the ephemeral path (§4). Producers need the `stream` schema installed and
nothing else running — publishing into a void is legal; the log holds the messages.

## 3. Bindings and relays

A binding is a row: `cb_stream_binding (pattern, destination_kind, destination,
start_position)`. Patterns keep today's grammar (`?` single token, `*` tail) and
today's matcher — port `topic_trie.go` as-is; matching happens **relay-side in Go**,
not in SQL (the trie is built, tested code; SQL gets at most a cheap prefix
prefilter).

A **relay** is one ordered consumer group on the root stream per destination,
running in any worker process (leader-elected the same way as the sequencer). Per
batch, in one transaction: match topics → write matches to the destination → advance
the relay cursor. Same-database writes make this **exactly-once materialization**
(README guarantees table) — destinations never see duplicates from the relay itself.

| destination_kind | Relay writes to | Consumed by |
|---|---|---|
| `stream` | another stream (work or ordered) | worker pools (01 §5) |
| `flow` | the flow's event stream: a `run_requested` event | the engine (03) |
| `inbox` | `cb_wire_inbox` rows, identity from a topic segment or payload field | the inbox (04) |

Notes:

- Direct cursor consumers on the root stream (no relay, no destination store) are
  allowed — that's just an ordered group with a topic filter (01 §4). They are the
  one case where a laggard pins shared storage, which the age cap bounds (01 §10).
- Binding changes take effect from the relay's next batch. New bindings choose
  `start_position` (tail | begin | ordinal) — the late-binding/replay story.
- The current `Bind(queue, pattern)` API survives with the same name and idempotent
  semantics; it now writes a binding row instead of routing at publish time.

## 4. The ephemeral path (wire)

wire never touches storage: `Publish` fires `pg_notify` inside the transaction, so
Postgres delivers it **on commit** — the push-only-on-commit property is free.
The kernel's notifier (today's `notifier.go`, reused) holds the one LISTEN
connection per process and fans out to wire's in-process subscribers (04). Payloads
above the 8000-byte NOTIFY limit send topic-only; wire re-pulls state — same
discipline as today.

## 5. Build checklist

1. Ensure the `bus` stream in the kernel migration; `catbird.Publish` facade over
   `stream.Append` + NOTIFY.
2. `cb_stream_binding` DDL + `Bind`/`Unbind` (idempotent, same signatures as today).
3. Relay runner: leader election, batch match (ported trie), per-kind writers,
   exactly-once cursor advance. `flow` and `inbox` kinds land with 03/04; `stream`
   kind lands first.
4. Tests: publish-then-rollback delivers nothing anywhere (the LiveView property);
   relay crash mid-batch produces no duplicates in destinations (exactly-once);
   late binding with `start_position: begin` replays retained history; a binding
   added after publish sees history, not just future.
