#!/usr/bin/env -S npx tsx
// A catbird jobs worker with no SDK: the whole contract is four SQL calls —
// claim, start, complete or fail — plus extend to keep a slow handler's
// lease alive. The handler below takes three seconds of "work" against a
// one-second claim TTL and still finishes, because the worker extends
// between work slices; a worker that dies mid-handler stops extending and
// the step falls to another worker. Releasing unstarted steps on shutdown
// is polite but optional: a lease nobody extends lapses on its own.
//
// Run with: npm install && npm start
import { Client } from "pg";
import { hostname } from "node:os";

const DSN = process.env.CB_DSN ?? "postgres://postgres:postgres@localhost:5432/cb_tst";
const QUEUES = ["ts_demo"];
const WORKER = `${hostname()}_ts_${process.pid}`;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type Handler = (
  conn: Client,
  runId: string,
  stepId: string,
  inp: Record<string, any>,
) => Promise<Record<string, unknown> | null>;

async function slowHello(conn: Client, runId: string, stepId: string, inp: Record<string, any>) {
  for (let slice = 0; slice < 6; slice++) { // six half-second slices, extending after each
    await sleep(500);
    const held = (await conn.query<{ run_id: string; step_id: string }>(
      "SELECT run_id, step_id FROM cb_job_extend($1, $2)", [QUEUES, WORKER],
    )).rows;
    const mine = held.some((step) => step.run_id === runId && step.step_id === stepId);
    if (!mine) return null; // taken over or canceled; a late report would change nothing
  }
  return { greeting: `hello ${inp.name}`, worker: WORKER };
}

const HANDLERS: Record<string, Handler> = { ts_hello: slowHello };

const conn = new Client({ connectionString: DSN });
await conn.connect();

for (;;) {
  const steps = (await conn.query(
    "SELECT run_id, step_id, name FROM cb_job_claim($1, $2)", [QUEUES, WORKER],
  )).rows;
  if (steps.length === 0) {
    await sleep(1000);
    continue;
  }
  for (const { run_id: runId, step_id: stepId } of steps) {
    const started = (await conn.query(
      "SELECT name, input, signal_input, attempt FROM cb_job_start($1, $2, $3)",
      [runId, stepId, WORKER],
    )).rows[0];
    if (started.name === null) continue; // finished, taken over, or given up: move on
    const { name, input, attempt } = started;
    let out: Record<string, unknown> | null;
    try {
      out = await HANDLERS[name](conn, runId, stepId, input ?? {});
    } catch (err) {
      await conn.query(
        "SELECT cb_job_fail($1, $2, $3, $4)", [runId, stepId, attempt, String(err)],
      );
      continue;
    }
    if (out !== null) {
      await conn.query(
        "SELECT cb_job_complete($1, $2, $3, $4, $5, $6)",
        [runId, stepId, attempt, JSON.stringify(out), null, JSON.stringify(out)],
      );
    }
  }
}
