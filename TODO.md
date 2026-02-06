# Catbird TODO

## Architecture & Design
- [ ] Fair queueing? https://docs.hatchet.run/blog/multi-tenant-queues

## Worker & Execution
- [ ] Pause tasks - wait for signal
- [ ] RunTaskWait / RunFlowWait long polling
- [ ] Add queue, task, flow and step description fields
- [ ] Improve graceful shutdown

## Features
- [ ] Schemas for task/flow input/output? https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html
- [ ] Flow onfail task (error handling in workflows)?
- [ ] Deduplication strategies (now only execution deduplication)