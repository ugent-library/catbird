[![Go Reference](https://pkg.go.dev/badge/github.com/ugent-library/catbird.svg)](https://pkg.go.dev/github.com/ugent-library/catbird)

![CatBird](catbird-banner.svg "CatBird banner")

Postgres based message queue with task and workflow runner.

## Acknowledgments

SQL code is mostly taken or adapted from the excellent [pgmq](https://github.com/pgmq) and [pgflow](https://github.com/pgflow-dev/pgflow) projects.

## TODO

* worker concurrency
* fair queueing? https://docs.hatchet.run/blog/multi-tenant-queues
* finish dashboard
* type safe task/flow input/output
* split tasks into standalone tasks and step tasks?
