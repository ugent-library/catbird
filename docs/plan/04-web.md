# 04 — wire: async web delivery

One package, two storage stories (D12). The vision said wire and the inbox share
no machinery. That was already false in the current code: they share Fragment
rendering, token auth, and the poll transport (PR #41). The honest claim, and the
design rule: **independent storage and delivery, shared presentation.** The
package is `wire` and it depends on the kernel only. (It used to optionally
import `stream` to register an `inbox` relay kind; relays died with routing —
D29 — and inbox rows are written explicitly by handlers holding the identity.)

## 1. Shape

```
wire/
├── ephemeral: SSE hub, presence, pg_notify subscriber   (today's wire.go — ports nearly whole)
├── inbox:     durable per-identity store + poll/push    (today's notifications.go, extended)
└── shared:    Fragment rendering, tokens, ServePoll     (explicitly shared internals)
```

Framework positioning (README amendment 1): the surface is SSE + HTML fragments +
JSON, aimed at server-rendered apps generally. htmx appears in examples only.

## 2. Ephemeral (wire proper)

Ports as-is: topics, SSE handler, tokens, presence. Two changes. First, subscribe
to the kernel notifier instead of owning a LISTEN connection. That means one
connection per process, shared with the assigner/ticker wakeups. Second,
push-on-commit is inherited from `pg_notify` semantics (02 §4). There is no
storage, no cursor, and delivery is at-most-once. A disconnected browser misses
ephemeral pushes. That is by design: filling that gap is the inbox's job.

## 3. Inbox — own table, not the log (D13)

`cb_wire_inbox (identity, id, topic, payload, created_at, seen_at, read_at,
expires_at?)`, plus the existing token/poll machinery. It deliberately does **not**
store rows on the substrate's log. The log's retention floor is the lowest cursor,
and the log can only drop rows below that floor. Inbox cursors would be
per-identity, and most users sit idle. One dormant account would pin partitions
forever. The vision said "the inbox can ride on the substrate". That is amended
(README #10): the inbox is its **own identity-keyed store** (D13). Handlers
write rows directly via `wire.NotifyDurable` — the handler that finishes a job
knows exactly which user asked for it, so identity is a value in its hand, not
something extracted from a topic (D29). Storage and retention stay
identity-local.

**seen / read distinction** (your note). Three timestamps, two verbs:

| State | Meaning | Set by |
|---|---|---|
| delivered | row exists (created_at) | `wire.NotifyDurable` |
| **seen** | rendered in the client's list — clears the badge | `MarkSeenUntil(identity, watermark)` — the watermark API from PR #41, unchanged |
| **read** | user opened/acted on this item | `MarkRead(identity, id)` / `MarkReadUntil` — per-item, **new** |

Unseen count drives badges. Unread drives item styling. Retention resolves the
open seen-row follow-up from the durable-notifications work. Delete a row when
`read_at` is older than R, when `seen_at` is older than S, or when its age passes
the hard cap A. Defaults: R 30d < S 90d < A 365d, per-app config. `expires_at`
still wins when set. The wait-until-seen guarantee from #39 survives as: no
deletion of unseen rows before A.

## 4. Durable push — built-in, optional (D12)

Your note, adopted. The vision rejected *baking durable push into wire*. The right
resolution is composition **shipped in the box**:

```go
wire.NotifyDurable(ctx, tx, identity, topic, payload)
// = inbox insert + pg_notify nudge.
// The insert is transactional: atomic with the caller's writes.
// Connected clients re-pull the inbox on the nudge; offline clients find
// the row on their next poll.
// Exactly-once in the store, at-most-once on the nudge.
```

Each half remains independently usable. `wire.Notify` stays public for
ephemeral-only pushes, and direct inbox reads stay public too. The SSE layer
learns nothing about scheduling or messaging. The helper is ten lines, not a
subsystem.

## 5. Build checklist

1. Port `wire.go`/`wire_token.go` onto the kernel notifier; keep the public API.
2. Inbox DDL (`cb_wire_*`, own goose table); port `notifications.go`; add
   `read_at` + `MarkRead`/`MarkReadUntil`; retention janitor on the kernel ticker.
3. `NotifyDurable` helper.
4. Shared internals folder for Fragment/tokens/ServePoll — one implementation,
   both transports.
5. Tests: port `wire_test.go` + `notifications_test.go`; new: seen vs read
   independence (marking seen never marks read), retention tiers incl.
   wait-until-seen, `NotifyDurable` rollback delivers neither row nor nudge,
   offline client catches up via poll after missed SSE.
