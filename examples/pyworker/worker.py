#!/usr/bin/env python3
# A catbird jobs worker with no SDK: the whole contract is four SQL calls —
# claim, start, complete or fail — plus extend to keep a slow handler's
# lease alive. The handler below takes three seconds of "work" against a
# one-second claim TTL and still finishes, because the worker extends
# between work slices; a worker that dies mid-handler stops extending and
# the step falls to another worker. Releasing unstarted steps on shutdown
# is polite but optional: a lease nobody extends lapses on its own.
import json, os, socket, time
import psycopg

DSN = os.environ.get("CB_DSN", "postgres://postgres:postgres@localhost:5432/cb_tst")
QUEUES = ["py_demo"]
WORKER = f"{socket.gethostname()}_py_{os.getpid()}"


def slow_hello(conn, run_id, step_id, inp):
    for _ in range(6):  # six half-second slices, extending after each
        time.sleep(0.5)
        held = conn.execute(
            "SELECT run_id, step_id FROM cb_job_extend(%s, %s)", (QUEUES, WORKER)
        ).fetchall()
        if (run_id, step_id) not in held:
            return None  # taken over or canceled; a late report would change nothing
    return {"greeting": f"hello {inp['name']}", "worker": WORKER}


HANDLERS = {"py_hello": slow_hello}

with psycopg.connect(DSN, autocommit=True) as conn:
    while True:
        steps = conn.execute(
            "SELECT run_id, step_id, name FROM cb_job_claim(%s, %s)", (QUEUES, WORKER)
        ).fetchall()
        if not steps:
            time.sleep(1.0)
            continue
        for run_id, step_id, job in steps:
            name, inp, signal_input, attempt = conn.execute(
                "SELECT name, input, signal_input, attempt FROM cb_job_start(%s, %s, %s)",
                (run_id, step_id, WORKER),
            ).fetchone()
            if name is None:
                continue  # finished, taken over, or given up: move on
            try:
                out = HANDLERS[name](conn, run_id, step_id, inp or {})
            except Exception as e:
                conn.execute(
                    "SELECT cb_job_fail(%s, %s, %s, %s)", (run_id, step_id, attempt, repr(e))
                )
                continue
            if out is not None:
                conn.execute(
                    "SELECT cb_job_complete(%s, %s, %s, %s, %s, %s)",
                    (run_id, step_id, attempt, json.dumps(out), None, json.dumps(out)),
                )
