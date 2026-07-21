# tsworker

A catbird jobs worker in one file, no SDK — just the SQL calls a worker
makes: `cb_job_claim`, `cb_job_start`, then `cb_job_complete` or
`cb_job_fail`, plus `cb_job_extend` to keep a slow handler's lease alive.
The `ts_hello` handler works for about three seconds against a one-second
lease and still finishes, because it extends between work slices. The
comment at the top of [worker.ts](worker.ts) walks through the contract.

## Requirements

- Node 18+. Install the dependencies (`pg`, and `tsx` to run the
  TypeScript): `npm install`.
- PostgreSQL on `localhost:5432`. From the repo root, `docker compose up -d`
  starts one.
- Catbird's jobs schema applied to `cb_tst` — the database the worker
  connects to by default (override with `CB_DSN`). Catbird applies its own
  schema on boot, so running `./scripts/test.sh` once, or the
  `examples/orders` app, puts the schema in `cb_tst`. The worker itself only
  issues the runtime SQL calls; it does not manage the schema.

## Define the queue and job

The worker claims from a queue that must already exist, and runs a job whose
name matches a handler it knows. Define both once, against the same endpoint
the worker connects to:

```sh
psql "postgres://postgres:postgres@localhost:5432/cb_tst"
```

```sql
-- 1s lease, so the ~3s handler must keep extending to survive — the
-- behaviour this example exists to show. Drop the interval for the stock
-- 30s lease, under which a 3s handler would finish without extending.
SELECT cb_job_define_queue('ts_demo', '1 second');
SELECT cb_job_define('ts_hello', 'ts_demo');
```

## Run the worker

```sh
npm start
```

It polls `ts_demo` and stays quiet until there is work.

## Send it a job

In another shell:

```sql
SELECT cb_job_run('ts_hello', '{"name": "you"}');
```

## Check the result

The worker prints nothing (beyond the `npm start` banner), so read the
outcome from the run row:

```sql
SELECT status, output
FROM cb_job_runs
WHERE job = 'ts_hello'
ORDER BY id DESC
LIMIT 1;
--  status    | output
-- -----------+-------------------------------------------------------
--  completed | {"greeting": "hello you", "worker": "myhost_ts_12345"}
```

Reaching `completed` proves the extend worked: the handler ran longer than
the one-second lease, so without extending, the lease would have lapsed and
the step been handed to another worker.
