# 04 — wire: delivering events to people

Wire is the browser boundary: server-rendered fragments over SSE, a
durable per-recipient inbox read by poll, and presence. It owns no
events. The model, ratified 2026-07-30 (D46), in one rule:

**Every message is a row; there is no rowless delivery.** A row is one of
three kinds, each with its own lifecycle:

| Row | Lives in | Lifecycle | Meaning |
|---|---|---|---|
| relayed stream message | the log (`cb_stream_messages`) | append, kept for the stream's retention | a fact of the system |
| inbox row | `cb_wire_inbox` | kept until read or expired | a thing *for* someone |
| presence row | `cb_wire_presence` | newest wins, evaporates on silence | where someone *is* |

The NOTIFY channels carry **addresses, never content**: `{stream, pos,
topic}` for a relayed message, `{recipient}` for an inbox nudge,
`{topic}` for a presence change. The receiving wire fetches the row
(`cb_stream_fetch` — the one log read wire makes), renders it through the
registry, and pushes a frame. Consequences, each load-bearing:

- **No replay, no resume, no SSE ids.** Anything a client misses is
  refetchable, because everything is a row — live frames are a latency
  optimization over refetch, never the source of truth. An SSE id would
  promise resume, so none is ever sent.
- **No size rules anywhere.** Payloads never ride a NOTIFY frame, so the
  8000-byte limit stays an internal constant of one hop nobody sees.
- **Reconnect is refetch.** A page that reconnects re-pulls the state it
  renders (its inbox, its presence rooms, its own app endpoints); there
  is nothing else to catch up on.

The two products are the token's two claims: **topics** grant the live
feed (at-most-once, while connected), **recipient** grants the inbox
(exactly-once, presence-independent). Module conventions apply whole
(D41): own migrations (`cb_wire_migrations`), own `Conn`, opts-struct
surface, every SQL name `cb_wire_*`, two fixed NOTIFY channels (`cbw`,
`cbw_inbox` — channels scale with the declared catalog, payloads carry
runtime coordinates, D45). Wire's Go depends on the kernel and `notify`,
never on `streams` or `jobs`; its one composition with streams is SQL,
through the stream module's public API (`cb_stream_define_cursor`,
`cb_stream_read`, `cb_stream_fetch`), the D41 direction.

The old root-schema names (`cb_notify`, `cb_notifications`, the `cb_wire`
channel) stay live beside this module until M6 — raven's `record_events`
trigger calls `cb_notify` in production — and nothing here reuses them.

## 1. Relays — the log's messages, forwarded to people

A relay is the trigger shape pointed at people (`cb_wire_relays`: name
PK, stream, expires_after, created_at): every message on its stream that
matches its filter is forwarded, by the module's tick, in one transaction
per batch — no consumer code deployed, cross-language by construction.
The relay owns the cursor named after it on its stream; the cursor is the
filter's single home (topic pattern + condition, the D29 languages,
`$.recipients == "name"` included) and remembers how far delivery got.

Per matching message, two legs in that one transaction:

- **Live leg**: one `pg_notify` on `cbw` carrying the message's address.
  Every wire process fetches the row, renders it per connection, and
  pushes the frame to the connections whose token topics match. NOTIFY
  fires on commit — the push is once per message; the leg is at-most-once
  per connection, by contract.
- **Durable leg**: one inbox row per addressed recipient — the union of
  the `recipients` the publisher named on the message (02 §3) and every
  matching watch (§3). Insert and cursor advance share the commit, so
  inbox rows are **exactly-once** — the guarantee the hand-written Go
  loop could not give (a crash between `Send` and the cursor's own commit
  redelivered the batch; the orders demo measured that loop as more code
  than its whole job chain).

The relevance window is channel policy: `RelayOpts.ExpiresAfter`, an
interval anchored at each **message's** `created_at`. One knob sets and
filters: an inbox row expires that long after the event happened, and a
message already past its window writes no rows — a stalled relay catching
up must not flood inboxes with stale rows granted fresh windows
(delivery-time anchoring was rejected for exactly that). Failure policy:
declaration defects are refused at define (the filter compiles or the
call raises); a message with no topic is skipped quietly — wire routes
and renders by topic — and the cursor never wedges on log-borne content,
because a bad message is in an immutable log and no deploy removes it.

`wire.DefineRelay(ctx, conn, name, stream, RelayOpts{Topic, Condition,
StartPos, ExpiresAfter})` — one verb, D26 semantics, an identical
declaration writes nothing; `DeleteRelay` removes relay and cursor.

## 2. The inbox — own table, not the log (D13)

`cb_wire_inbox (id, recipient, topic, payload, created_at, seen_at,
read_at, expires_at)` — `id` is the poll cursor; `payload` is the event
itself, exactly as published, rendered at read time (the uniform event
model: the demo once minted an `inbox.order` topic with pre-rendered text
purely to compensate for a split renderer story — with rows carrying the
event, the feed's renderer serves the inbox unchanged). The inbox
deliberately does **not** store rows on the substrate's log: log
retention is positional and one dormant account would pin partitions
forever; inbox retention is per-person state (D13, unchanged).

Writes arrive two ways. A relay's durable leg (§1) covers events whose
audience is data — named recipients, watches. `wire.Send(ctx, conn,
recipient, topic, payload, SendOpts{ExpiresAt})` — `cb_wire_send`, wire's
one write — covers the author who holds the recipient in hand, in the
author's transaction: the row commits with the app's writes, the nudge
fires only on commit, a rollback delivers neither. Exactly-once in the
store, at-most-once on the nudge.

**The nudge is recipient-addressed in the payload of one fixed channel**
(`cbw_inbox`): wire keeps a recipient → connections map beside the topic
trie and writes one reserved `event: inbox` frame (empty data) to that
recipient's connections; the client answers by re-pulling its poll
endpoint. A topic-addressed nudge would ping every holder of a shared
topic grant, and a payload-carrying nudge would double-render (a frame
carries no row id to ack). Offline clients find the rows on their next
poll — the inbox is the catch-up path, the push is the fast lane.

**seen / read, timestamps not statuses**: `seen_at` (rendered in the
list; unseen count drives badges) via `MarkSeenUntil`/`MarkSeen`,
`read_at` (opened or acted on; drives styling) via
`MarkRead`/`MarkReadUntil`. Reading implies seeing — an opened row must
leave the badge — and every stamp keeps its first value. **Retention**:
explicit `expires_at` always wins, seen or not; otherwise read older than
R, seen older than S, created older than A (defaults 30/90/365d,
`wire.TickerOpts` arguments — wire has no retention declarations). A row
never seen with no expiry waits to be seen, the full A.

## 3. Watches — subscriptions into the inbox

`cb_wire_subscriptions (recipient, pattern, expires_at)`: "matching
relayed events go to my inbox until then". Patterns are prefix-only — an
exact topic or `prefix.#` (`p.#` covers `p` itself, as in every topic
language here); `*` is refused. Matching inverts into a B-tree probe: the
deliverer expands a message's topic into its covering patterns (the
topic, `#`, every prefix + `.#`) and looks those up — cost follows the
topic's length, never the table's size. Conditions on watches are
deliberately absent: the conditional case is the publisher's, named as
`recipients`. `wire.Subscribe(ctx, conn, recipient, pattern,
SubscribeOpts{ExpiresAt})`, `Unsubscribe`; lapsed watches stop matching
at once and the prune removes the rows.

Addressing, in one line each: **live visibility** is the token's topics
and nothing else; **inbox addressing** is named recipients ∪ watches ∪ `Send`;
computed fan-out with transformation stays a handler.

## 4. Presence — where people are (separate from messages)

`cb_wire_presence (topic, recipient, payload, expires_at)`: one row per
(topic, person), newest payload wins, gone when the heartbeat stops.
Presence is state, not messages — nothing kept, nothing addressed — so
none of the message machinery applies, renderers included: the renderable
unit is the *set* ("everyone on this record, and their fields"), so the
app owns the handler and template, and wire ships mechanism only.
`wire.Appear(ctx, conn, topic, recipient, payload, ttl)` upserts and
re-arms; watchers are nudged (an empty frame named after the topic) only
when something visible changed — arrival, payload change, back from
expired, leave — never on a bare heartbeat re-arm. `Disappear` is the
polite leave; silence works too. `PresenceAt` returns the live rows;
expired rows never render, and the prune nudges each topic it sweeps so
pages drop people whose laptop closed. Presence topics are ordinary
topics named by app convention (`record.123.presence`): token grants and
SSE event names work unchanged, and a presence topic never collides with
an event topic.

Rowless presence was the old design's grave: a joining client saw an
empty room until heartbeats trickled in, and the cross-node change signal
never worked. Rows dissolve both — join is a refetch.

## 5. Transports and the frame contract

Frames are **plain SSE, nothing htmx-flavored**: `event:` is the topic,
`data:` is the rendered fragment — or the payload JSON when no renderer
matches or the connection asked `?raw=1`. One renderer registry
(`Render`, typed `Render[T]`), read-time, request-aware, serving every
surface the same fragment. The htmx SSE extension, hand-rolled
`EventSource` listeners (`addEventListener` + `htmx.process`), and the
shipped glue (`wire.js`, served by `ServeScript`: SSE → DOM CustomEvents
`wire:<topic>` + declarative `data-wire-swap` + `htmx.process`) are three
consumers of that one contract. One recorded constraint shapes all glue:
EventSource has no wildcard for named events, so a page names the topics
it uses — the token already bounds what can arrive, and envelope tricks
would break the extension's `sse-swap`.

The idiom split: **log events swap** (the frame carries the fragment);
**state triggers refetch** (inbox nudge, presence nudge — the row is the
data). Poll is the same products over plain GET: `ServePoll` renders the
recipient's unseen rows through the registry (JSON on `Accept:
application/json`), cursor in the `X-Wire-Cursor` header, pure read —
acks flow only through the mark verbs, so tabs converge. An htmx page
degrades from push to poll by adding `every 30s` to the same trigger
list. Tokens: AES-256-GCM, claims = topics + recipient + expiry.

## 6. What died with the model (do not re-propose without new evidence)

- **`w.Notify` / `cb_wire_notify`, `Listen`, `sent_by`** — the rowless
  bus. Its one production caller (raven's refresh ping) already writes
  rows (`record_events`) and becomes a stream publish + relay at M6;
  server-side Go handlers that want events are stream consumers with
  cursors, not wire listeners — wire was being a second, worse bus.
- **Poll serving "the relayed feed after a client position", SSE
  `Last-Event-ID` resume, `cb_stream_read_after`** — the feed trying to
  be a thing with positions. Log history belongs to the log's own tools
  (a cursor, SQL) beside wire, not through it; the cursor-borrowing read
  died with the requirement.
- **Oversize machinery** (inline-when-fits, chunking, a buffer table,
  forbidding big payloads) — moot once frames are addresses; the buffer
  table was a shadow log, chunking was reassembly on an at-most-once
  hop, and a size cap would have promoted NOTIFY's constant into the app
  contract, measured on the storage form instead of the delivered
  fragment.
- **A first-class feed/filter entity** — nothing resumes a feed, so
  nothing needs the noun.

## 7. Build record

Built 2026-07-30, with the module (all pre-release, edited in place):
recipient vocabulary throughout (token claim `r`); `cb_wire_send`
(payload jsonb); relays + `cb_wire_relay_deliver`; subscriptions +
`_cb_wire_topic_patterns`; presence + its prune-with-nudge; the address
frames and the fetch in wire's dispatch; `ServeScript`/`wire.js`;
`ServePoll` JSON mode; `wire.TickerOpts.Notifier` + relay tick
(fixed-at-start stream set, the worker convention). The orders demo runs
on a declared relay, named recipients and the glue. `docs/sql-api.md`
carries the contract.
