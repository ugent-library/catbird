# 04 — wire: async web delivery

One package, two storage stories (D12). The vision said wire and the inbox share
no machinery. That was already false in the current code: they share Fragment
rendering, token auth, and the poll transport (PR #41). The honest claim, and the
design rule: **independent storage and delivery, shared presentation.** The
package is `wire` and it depends on the kernel and the `notify` package — never
on `streams` or `jobs` (D45). (It used to optionally import `stream` to register
an `inbox` relay kind; relays died with routing — D29 — and inbox rows are
written explicitly by handlers holding the identity.)

Module conventions apply whole (D41): own migrations (`wire/migrations`, goose
table `cb_wire_migrations`), own `Conn`, opts-struct surface, every SQL name
module-prefixed (`cb_wire_*`), and two NOTIFY channels of wire's own — `cbw`
carries the event JSON `{sent_by, topic, message}`, `cbw_inbox` carries an
identity. The old names stay live in the old schema until M6 — raven's
`record_events` trigger calls `cb_notify` in production, and old and new share
one database — so nothing here reuses them: not the
`cb_wire_nodes`/`cb_wire_presence` tables, not the
`cb_notify*`/`cb_mark_seen*` functions, not the `cb_wire` channel.

## 1. Shape

```
wire/
├── ephemeral: SSE hub, Listen handlers, notifier subscriber    (today's wire.go, minus presence)
├── inbox:     durable per-identity store + poll + nudge        (today's notifications.go, extended)
└── shared:    Fragment rendering, tokens, ServePoll, the trie  (explicitly shared internals)
```

Framework positioning (README amendment 1): the surface is SSE + HTML fragments +
JSON, aimed at server-rendered apps generally. htmx appears in examples only.

## 2. Ephemeral (wire proper)

Ports: topics and the in-process trie dispatch, the SSE handler, tokens,
`Listen`/`Render`. Wire subscribes to its two channels once, at `Start` — the
trie routes topics in-process, so the channel count never grows with topics,
subscribers or identities (D45). The changes from today's wire.go:

- **The shared notifier replaces the owned LISTEN connection.** `Start` may run
  after the notifier's; both subscriptions land on the live connection. The
  notifier runs callbacks on the shared connection's goroutine, and wire hands
  that goroutine to nobody: both callbacks only enqueue into wire's own
  buffered dispatch channel (cap ~256), drained by one wire goroutine that
  runs the `Listen` handlers and the SSE fan-out. Overflow drops the event
  with a warning — ephemeral delivery is at-most-once by design, and an app
  `Listen` handler must never be able to stall every consume loop in the
  process.
- **Surface**: `wire.New(pool, secret, Opts{Notifier, Logger})`. A nil
  notifier is a working single-process configuration — local delivery and
  both HTTP surfaces function; only cross-process push needs the notifier.
  `Start` states the wake source at Info, symmetric wording: "pushing on
  notify across processes" / "pushing within this process; clients catch up
  by poll".
- **The `catbird.%` reserved-topic guard drops**: no system producer
  publishes on this bus — engine events ride `cbs_*`/`cbj_*`.
- **The 8000-byte NOTIFY limit is the caller's contract** (godoc on
  `wire.Notify`/`cb_wire_notify`): send a pointer to state, not the state;
  oversize raises in the caller's transaction.

**Presence does not port** — deferred, with its slot. No caller exists (raven,
dashboard and TUI use none of it; its only exerciser was its own test), and
its cross-node change signals never worked: both call sites name a `cb_notify`
parameter that does not exist and discard the error. When a customer arrives,
it returns additively as identity-keyed presence rows plus an instance
heartbeat (`cb_wire_presence`, `cb_wire_instances`) and a sweep tick.

## 3. Inbox — own table, not the log (D13)

`cb_wire_inbox (id, identity, topic, message, created_at, seen_at, read_at,
expires_at)` — `id` is a global identity column and the poll cursor; `message`
stays `text` (the same opaque substance as the ephemeral dialect; renderers
and apps decode — raven stores JSON strings in it); partial indexes serve the
unseen poll path and the expiry sweep. It deliberately does **not** store rows
on the substrate's log. The log's retention floor is the lowest cursor, and the
log can only drop rows below that floor. Inbox cursors would be per-identity,
and most users sit idle. One dormant account would pin partitions forever. The
vision said "the inbox can ride on the substrate". That is amended (README
#10): the inbox is its **own identity-keyed store** (D13). Handlers write rows
directly via `wire.NotifyDurable` — the handler that finishes a job knows
exactly which user asked for it, so identity is a value in its hand, not
something extracted from a topic (D29). Storage and retention stay
identity-local.

`collapse_key` does not port — no customer (raven passes only `ExpiresAt`).
Keep-newest collapse (the FCM semantics) is recorded as a deferred design; it
returns as one nullable column, one partial index and one write-time update.

**seen / read distinction.** Three timestamps, two verbs:

| State | Meaning | Set by |
|---|---|---|
| delivered | row exists (created_at) | `wire.NotifyDurable` |
| **seen** | rendered in the client's list — clears the badge | `MarkSeenUntil(identity, watermark)` / `MarkSeen(identity, ids)` — the PR #41 acks, unchanged |
| **read** | user opened/acted on this item | `MarkRead(identity, id)`, `MarkReadUntil(identity, id)` — **new** |

**Reading implies seeing**: an opened row must leave the badge count, so both
mark-read functions stamp `seen_at` too; every stamp keeps its first value.
`MarkRead` returns whether the row exists — marking an already-read row is a
no-op that still returns true (the idempotent-API rule). Unseen count drives
badges. Unread drives item styling.

**Retention** resolves the open seen-row follow-up from the durable-
notifications work. A row leaves the inbox when its explicit `expires_at`
passes, or when the identity is done with it: `read_at` older than R,
`seen_at` older than S, `created_at` older than A. Defaults R 30d < S 90d <
A 365d, per-app config (`wire.TickerOpts` — wire has no definitions table, so
the windows are arguments to `_cb_wire_prune_inbox`, the `cb_purge_task_runs`
precedent). The wait-until-seen guarantee from #39 survives as: **a row that
was never seen and has no `expires_at` is not deleted before A** — an explicit
`expires_at` always wins, seen or not (a stale prompt is not worth keeping).
The prune is one `DELETE` on wire's own tick (`wire.StartTicker`, no Notifier
field — nothing wakes retention); the inbox holds human-scale rows, so no
batching until a measurement asks for it. `NULL` timestamps fail the age
comparisons, so unread and unseen rows pass through their tiers untouched by
construction.

## 4. Durable push — built-in, optional (D12)

The vision rejected *baking durable push into wire*. The right resolution is
composition **shipped in the box**, and it lives in SQL so foreign-language
callers get all of it (the engine-in-SQL rule):

```go
wire.NotifyDurable(ctx, conn, identity, topic, message, opts) // Opts{ExpiresAt}
// = one call to cb_wire_notify_durable: the inbox insert + the nudge, one body.
// Callable in the caller's transaction: the insert is atomic with the app's
// writes, and NOTIFY fires on commit — a rollback delivers neither row nor nudge.
// Exactly-once in the store, at-most-once on the nudge.
```

**The nudge is identity-addressed in the payload of one fixed channel**:
`pg_notify('<schema>.cbw_inbox', identity)`. The inbox is identity-keyed, so
its wake is too (D45) — a topic-addressed nudge would ping every holder of a
shared topic grant (raven's durable topics are constants like
`task.batch_edit`), and a payload-carrying nudge would double-render, because
an SSE frame carries no row id to ack. Wire keeps an identity → connections
map beside the topic trie and writes one `event: inbox` frame (empty data;
`inbox` is a reserved event name) to that identity's connections; the client
answers by re-pulling its poll endpoint — `ServePoll` or a hand-rolled read
like raven's tray. Offline clients find the row on their next poll.

Each half remains independently usable. `wire.Notify` stays public for
ephemeral-only pushes, and direct inbox reads stay public too. The SSE layer
learns nothing about scheduling or messaging.

## 5. Build checklist

1. Module skeleton: `wire/migrations` + `cb_wire_migrations`,
   `MigrateUpTo`/`MigrateDownTo` (the streams/jobs shape), `wire.Conn`, and
   module copies of the topic trie + `matchTopic` (unexported in the root
   package; in-process pattern dispatch — the engine's matcher stays the
   streams SQL grammar). Channel names are built inline from
   `current_schema()`, as in streams and jobs.
2. Port `wire.go`/`wire_token.go` per §2: two notifier subscriptions, own
   dispatch goroutine, opts-struct constructor; presence out.
3. Inbox DDL + functions: `cb_wire_notify`, `cb_wire_notify_durable` (insert +
   nudge), `cb_wire_mark_seen_until`, `cb_wire_mark_seen`, `cb_wire_mark_read`,
   `cb_wire_mark_read_until`, `_cb_wire_prune_inbox`; port `notifications.go` —
   `ReadAt` in, `CollapseKey` out.
4. `wire.StartTicker`: the retention tick.
5. Tests: port `wire_test.go` + `notifications_test.go` (presence and collapse
   tests retire with their features); new: reading implies seeing; the
   `MarkReadUntil` watermark; the prune tiers including unseen-survives-until-A
   and `expires_at`-wins; a rolled-back `NotifyDurable` delivers neither row
   nor nudge; an offline client catches up via poll after missed SSE; the
   nudge reaches only its identity's connections; two Wires on one notifier
   cross-deliver; dispatch overflow drops without stalling the notifier;
   nil-notifier single-process delivery.
6. `docs/sql-api.md` gains the wire contract.
