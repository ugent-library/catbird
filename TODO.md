# Catbird TODO

## Architecture & Design
- [ ] Fair queueing? (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] Database coordinated scheduling (see SCHEDULING_ADVANCED.md)
 
## Worker & Execution
- [ ] RunTaskWait / RunFlowWait long polling
- [ ] Add queue, task, flow and step description fields

## Features
- [ ] Metrics
- [ ] Schemas for task/flow input/output? (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)
- [ ] OnFail handlers?
- [ ] Dynamic step subtasks execution? (see pgflow)

## Client implementations
- [ ] Full Typescript client implementation (with workers)
- [ ] Slim client implementations (no workers, facade only) for popular languages
