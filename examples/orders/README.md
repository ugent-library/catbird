# The orders demo

This is catbird's whole pitch on one screen. You click a button, one
message is published inside a database transaction, and four things
react to the commit: a worker pool picks the order up, a job chain runs
it, your browser gets a live push, and a notification lands in your
inbox. There is a second button that rolls the transaction back — click
it and watch nothing happen, which is the point.

## Run it

You need a local PostgreSQL. The repository's compose file works:

```bash
docker compose up -d
go run ./examples/orders
```

Then open <http://localhost:8080>.

By default the demo connects to the test database
(`cb_tst` on `localhost:5432`). Point it elsewhere with `CB_DSN`:

```bash
CB_DSN="postgres://user:pass@somehost:5432/somedb" go run ./examples/orders
```

On start it applies the streams, jobs and wire migrations, declares
everything it needs, and prints a line per consumer saying how it wakes
up. All of that is idempotent — restart it as often as you like.

## What to do

**Click "Place order".** Watch two places at once:

- **The terminal** logs the fulfilment consumer picking the order up,
  then each item being picked, then the order completing.
- **The live feed** in the browser shows the same story as it happens:
  a blue "order placed" line, a grey line per picked item, and a green
  "order processed" line when the last pick is done.
- **The inbox** below the feed gets a new entry and its badge goes red —
  without you reloading anything. Click **"Mark all read"** and the
  badge empties; the rows stay in the database until retention removes
  them, but you're done with them.

**Click "Place order, then roll back".** The order row is written and
the message is published — and then the transaction aborts. No feed
entry, no job, no inbox row, no log line. Nothing downstream ever sees
an uncommitted message; there is no cleanup code making that true, the
transaction is the cleanup.

Place a few orders quickly in a row — the picks interleave, because the
job steps of different orders compete for the same workers.

## What just happened

One `Publish` call inside your transaction, four independent readers of
its commit:

1. The **fulfilment subscription** is the worker-pool leg: competing
   consumers, at-least-once, retries with backoff. The demo just logs;
   a real app would ship the box.
2. The **trigger** births a `process_order` job run with the message
   payload as its input. That run fans out one `pick_item` step per
   item, and `confirm_order` runs after all of them finish — a plan
   that grows at runtime, not a pre-declared graph.
3. The **relay** — one declared row, no consumer code — forwards each
   matching message to the web: a live frame to every connected browser
   whose token covers the topic. The frame carries only the message's
   address; the wire fetches the row, renders it and pushes the
   fragment.
4. The same relay writes a **durable inbox row** for every recipient the
   publisher named on the message — `order.placed` names `demo-user`.
   The row carries the event itself, rendered by the same renderer as
   the live feed, and its commit nudges the browser to re-pull — that's
   the red badge. If nobody is connected, the row waits; the inbox is
   the catch-up path, the push is just the fast lane.

This is the integration model from `docs/vision.md` §4. The four `Bind`
lines sketched there became: a subscription, a trigger, and a relay —
all rows, all declared. Routing didn't survive the design work — readers
declaring what they want did.

## Poke around

Everything is rows. While the demo runs:

```sql
-- the demo's own table, committed together with the publish
SELECT * FROM demo_orders ORDER BY id DESC LIMIT 5;

-- the stream, with the rolled-back orders conspicuously absent
SELECT pos, topic, payload FROM cb_stream_messages
WHERE stream = 'orders' ORDER BY pos DESC LIMIT 5;

-- the job chain: one run per order, its steps and their outcomes
SELECT r.id, r.status, s.name, s.status
FROM cb_job_runs r JOIN cb_job_steps s ON s.run_id = r.id
WHERE r.job = 'process_order'
ORDER BY r.id DESC, s.id LIMIT 12;

-- the inbox, with seen/read stamps moving as you click
SELECT id, topic, payload, seen_at, read_at FROM cb_wire_inbox
WHERE recipient = 'demo-user' ORDER BY id DESC LIMIT 5;
```

Two more things worth trying:

- **Kill the demo mid-order** (Ctrl-C right after placing one) and
  start it again. The claimed work lapses, another start finishes it —
  nothing is lost, the database was the coordinator all along.
- **Run a second copy** on another port (edit `addr`) — both browsers
  get every push, and the workers of both processes compete for the
  same steps.
