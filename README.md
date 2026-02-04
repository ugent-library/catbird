[![Go Reference](https://pkg.go.dev/badge/github.com/ugent-library/catbird.svg)](https://pkg.go.dev/github.com/ugent-library/catbird)

![CatBird](catbird-banner.svg "CatBird banner")

Postgres based message queue with task and workflow runner.

## Acknowledgments

SQL code is mostly taken or adapted from the excellent [pgmq](https://github.com/pgmq) and [pgflow](https://github.com/pgflow-dev/pgflow) projects.

## TODO

* split queues into queues and exchanges - don't store topic in message?
* move jitter factor to postgres queue definition?
* handle panics in worker
* pause tasks - wait for for signal
* RunTaskWait / RunFlowWait long polling
* add queue, task, flow and step description fields
* fair queueing? https://docs.hatchet.run/blog/multi-tenant-queues
* finish dashboard
* schemas for task/flow input/output https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html
* flow onfail task
