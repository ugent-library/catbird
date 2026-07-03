# 04 — wire: async web delivery

One package, two storage stories (D12). The vision's "wire and the inbox share no
machinery" was already false in the current code — they share Fragment rendering,
token auth, and the poll transport (PR #41). The honest claim, and the design rule:
**independent storage and delivery, shared presentation.** Package `wire`, depends
on the kernel only; `stream` optional (spine glue).

## 1. Shape

```
wire/
├── ephemeral: SSE hub, presence, pg_notify subscriber   (today's wire.go — ports nearly whole)
├── inbox:     durable per-identity store + poll/push    (today's notifications.go, extended)
└── shared:    Fragment rendering, tokens, ServePoll     (explicitly shared internals)
```

Framework positioning (README amendment 1): the surface is SSE + HTML fragments +
JSON — server-rendered apps generally. htmx appears in examples only.

## 2. Ephemeral (wire proper)

Ports as-is: topics, SSE handler, tokens, presence. Changes: subscribe to the
kernel notifier instead of owning a LISTEN connection (one connection per process,
shared with the sequencer/ticker wakeups); push-on-commit is inherited from
`pg_notify` semantics (02 §4). No storage, no cursor, at-most-once — a disconnected
browser misses ephemeral pushes, by design; that gap is the inbox's job.

## 3. Inbox — own table, not the log (D13)

`cb_wire_inbox (identity, id, topic, payload, created_at, seen_at, read_at,
expires_at?)`, plus the existing token/poll machinery. It deliberately does **not**
store rows on the substrate's log: per-identity cursors with mostly-idle users are
incompatible with cursor-floor retention (one dormant account would pin partitions
forever). The vision's "the inbox can ride on the substrate" is amended (README #10)
to: the inbox rides the **spine** — an `inbox` relay kind (02 §3) writes rows here —
while storage and retention stay identity-local.

**seen / read distinction** (your note). Three timestamps, two verbs:

| State | Meaning | Set by |
|---|---|---|
| delivered | row exists (created_at) | relay or direct `wire.NotifyDurable` |
| **seen** | rendered in the client's list — clears the badge | `MarkSeenUntil(identity, watermark)` — the watermark API from PR #41, unchanged |
| **read** | user opened/acted on this item | `MarkRead(identity, id)` / `MarkReadUntil` — per-item, **new** |

Unseen count drives badges; unread drives item styling. Retention (resolves the
open seen-row follow-up from the durable-notifications work): delete when
`read_at` older than R, or `seen_at` older than S, or age older than the hard cap A
(defaults R 30d < S 90d < A 365d, per-app config); `expires_at` still wins when set.
The wait-until-seen guarantee from #39 survives as: no deletion of unseen rows
before A.

## 4. Durable push — built-in, optional (D12)

Your note, adopted: the vision rejected *baking durable push into wire*; the right
resolution is composition **shipped in the box**:

```go
wire.NotifyDurable(ctx, tx, identity, topic, payload)
// = inbox insert (transactional, atomic with caller's writes)
// + pg_notify nudge; connected clients re-pull the inbox; offline clients
//   find it on next poll. Exactly-once in the store, at-most-once on the nudge.
```

Each half remains independently usable — `wire.Notify` (ephemeral only) and direct
inbox reads both stay public. Nothing about the SSE layer learns about scheduling
or messaging; the helper is ten lines, not a subsystem.

## 5. Build checklist

1. Port `wire.go`/`wire_token.go` onto the kernel notifier; keep the public API.
2. Inbox DDL (`cb_wire_*`, own goose table); port `notifications.go`; add
   `read_at` + `MarkRead`/`MarkReadUntil`; retention janitor on the kernel ticker.
3. `NotifyDurable` helper; `inbox` relay kind registration (02 §3).
4. Shared internals folder for Fragment/tokens/ServePoll — one implementation,
   both transports.
5. Tests: port `wire_test.go` + `notifications_test.go`; new: seen vs read
   independence (marking seen never marks read), retention tiers incl.
   wait-until-seen, `NotifyDurable` rollback delivers neither row nor nudge,
   offline client catches up via poll after missed SSE.
