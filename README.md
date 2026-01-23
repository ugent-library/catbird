[![Go Reference](https://pkg.go.dev/badge/github.com/ugent-library/catbird.svg)](https://pkg.go.dev/github.com/ugent-library/catbird)

![CatBird](catbird-banner.svg "CatBird banner")

Postgres based message queue with task and workflow runner.

## Acknowledgments

SQL code is mostly taken or adapted from the excellent [pgmq](https://github.com/pgmq) and [pgflow](https://github.com/pgflow-dev/pgflow) projects.

## TODO

* hide message for as long as task is running
* empty map step array folding
* map flow input
* pause tasks - wait for for signal
* RunTaskWait / RunFlowWait long polling
* add task and flow description fields
* add queue created_at field
* fair queueing? https://docs.hatchet.run/blog/multi-tenant-queues
* finish dashboard
* schemas for task/flow input/output
* split tasks into standalone tasks and step tasks?
* use split tables for task runs like queues?
* flow onfail task?
* put logger in Context?
