# 02 — spine: publish, topics, bindings, relays

The spine is not a module (D8). It is a **usage pattern of the substrate**: one
well-known root stream, topic filters, and relay consumers. This dissolves the
vision's open decisions 1 and 3: there is nothing left to place.

## 1. Fan-out-on-read (D8)

The vision drew `Publish` as N inserts, one per bound stream, inside the caller's
transaction. This plan inverts that. **Publish writes exactly one row** into a
topic-keyed root stream, named `bus` by default. Bindings are evaluated at *read*
time.

Why (recap of the design review):

- Publish is O(1) no matter how many bindings exist. The code path inside user
  request transactions gets cheaper, not amplified.
- Late binding becomes real. A new binding can start from history, not just the
  future, as far back as the root stream's retention reaches.
- Per-identity routing stops being binding-table sprawl. The vision's own example
  (`toInbox(id)`, `toWire("user."+id)`) interpolates runtime identities into
  bindings. That is a sign the identities were data all along. Under
  fan-out-on-read they are ordinary routing decisions inside a relay handler.
- The LiveView property survives untouched: nothing fires before commit. What
  changes is timing: the relay fills the queue milliseconds *after* commit
  instead of *during* it. That gap is relay lag.

Absent consumers (from the design conversation, recorded here): the durable thing
is the **cursor row**, not the process. A consumer that is subscribed but down
resumes from its cursor. A consumer that has not subscribed yet can replay within
retention. That is strictly better than fan-out-on-write, where an unrouted
message is gone forever. Destination stores absorb long-term absence. Work
streams and the inbox are such stores; each has its own retention. The root
stream only needs to retain relay lag plus the desired replay window. Default
root retention: age-cap, 7 days.

## 2. API

```go
// the five-minute API, in the root package
catbird.Publish(ctx, tx, "order.placed", order)          // one insert, atomic with tx
catbird.Publish(ctx, tx, "order.placed", order,
    catbird.WithKey("order-"+id),                        // dedup keep-oldest (01 §8)
    catbird.WithDelay(10*time.Minute))                   // relative — DB clock (01 §6)

catbird.Publish(ctx, tx, "record.embargo_lifted", rec,
    catbird.WithDeliverAt(rec.EmbargoEnd))               // absolute; not with WithDelay

catbird.Publish(ctx, tx, "digest.send", u.ID,
    catbird.WithKey("digest-"+u.ID),
    catbird.WithDelay(30*time.Second))    // held 30s; same-key publishes dedup (01 §8).
                                          // undo = flag it in app state; the handler
                                          // checks before sending (01 §6)
```

`Publish` is `stream.Append("bus", topic, …)` and nothing more. The append's own
per-stream notify *is* the ephemeral path: wire simply listens to the bus's
channel (§4). Producers need the `stream` schema installed and nothing else
running. Publishing into a void is legal; the log holds the messages.

## 3. Bindings and relays

A binding is a row: `cb_stream_bindings (pattern, destination_kind, destination,
start_position)`. Patterns keep today's grammar (`?` single token, `*` tail) and
today's matcher: port `topic_trie.go` as-is. Matching happens **relay-side in
Go**, not in SQL. The trie is built, tested code; SQL gets at most a cheap prefix
prefilter.

A **relay** is one cursor on the root stream per destination. Any worker may run
it; the cursor row's lock lets one work at a time. Each batch runs in one
transaction: match topics, write the matches to the destination, advance the
relay cursor. The writes land in the same database, so this is **exactly-once
materialization** (README guarantees table). Destinations never see duplicates
from the relay itself.

| destination_kind | Relay writes to | Consumed by |
|---|---|---|
| `stream` | another stream (work or ordered) | worker pools (01 §5) |
| `flow` | the flow's event stream: a `run_requested` event | the engine (03) |
| `inbox` | `cb_wire_inbox` rows, identity from a topic segment or payload field | the inbox (04) |

Notes:

- A consumer may read the root stream directly, with no relay and no destination
  store. That is just a cursor with a topic filter (01 §4). Such consumers are
  the one case where falling behind pins shared storage. The age cap bounds
  that (01 §10).
- Binding changes take effect from the relay's next batch. New bindings choose
  `start_position` (tail | begin | position). That choice is how late binding and
  replay work.
- The current `Bind(queue, pattern)` API survives with the same name and idempotent
  semantics. It now writes a binding row instead of routing at publish time.
- Relay *kinds* are registered, not imported. The relay runner exposes
  `RegisterRelayKind(name, writer)`. `stream` ships the `stream` kind; `flow`
  and `wire` register theirs at init when the app imports them. This keeps the
  README's dependency rule intact: no package reaches into another's tables.
- Only the binding that *creates* the destination's relay cursor gets its
  `start_position` honored. Later bindings to the same destination inherit the
  existing cursor. There is one cursor per destination, so two bindings cannot
  make it replay twice.
- The `inbox` kind extracts the identity by config, not convention. The binding
  row has an `identity_from` column: a topic segment index or a payload JSON
  path.

The relay itself is about a screenful of Go. It is an ordered consumer that
filters topics with the trie:

```go
// one per destination. Any node may run it; the cursor row's lock decides.
func runRelay(ctx context.Context, pool *pgxpool.Pool, dest Destination) error {
	matcher := trie.New(loadPatterns(dest)) // ported topic_trie.go; reload on
	                                        // binding-change notify
	return stream.Consume(ctx, pool, "bus", "relay_"+dest.Name,
		func(ctx context.Context, tx pgx.Tx, msgs []stream.Message) error {
			for _, m := range msgs {
				if !matcher.Match(m.Topic) { // wildcard match: Go-side (01 §4)
					continue
				}
				if err := dest.Writer.Write(ctx, tx, m); err != nil {
					return err // registered kind (RegisterRelayKind)
				}
			}
			return nil
			// commit: destination writes and cursor advance in one tx.
			// That is exactly-once materialization, the guarantee in the README table.
		})
}
```

## 4. The ephemeral path (wire)

wire never touches storage. Every append fires `pg_notify` on a channel named
after its stream, inside the transaction. Postgres delivers the notification
**on commit**, so the push-only-on-commit property is free. There is no channel
configuration: wire subscribes to the bus's channel, the same way the assigner
driver subscribes to every stream's. The notify follows the *actual append*. A
delayed publish does not notify at accept time; `_cb_stream_deliver_pending`
fires the notify when the message is due. A publish skipped by dedup does not
notify at all. The kernel's notifier holds the one LISTEN connection per process
and fans out to wire's in-process subscribers (04). It grows from today's
`worker_notifier.go`; see 05's file-name note. It arrives at M5 with wire (D17).
Before that the notifications simply have no listeners. A payload above the
8000-byte NOTIFY limit sends topic-only; wire re-pulls state. That is the same
discipline as today.

## 5. Build checklist

1. Ensure the `bus` stream in the kernel migration; `catbird.Publish` facade over
   `stream.Append` + NOTIFY.
2. `cb_stream_bindings` DDL + `Bind`/`Unbind` (idempotent, same signatures as today).
3. Relay runner: batch match (ported trie), per-kind writers,
   exactly-once cursor advance. `flow` and `inbox` kinds land with 03/04; `stream`
   kind lands first.
4. Tests: publish-then-rollback delivers nothing anywhere (the LiveView property);
   relay crash mid-batch produces no duplicates in destinations (exactly-once);
   late binding with `start_position: begin` replays retained history; a binding
   added after publish sees history, not just future.
