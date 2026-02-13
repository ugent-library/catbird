# Catbird TODO

## Architecture & Design
- [ ] Fair queueing? (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] Database coordinated scheduling (see SCHEDULING_ADVANCED.md)
- [ ] Allow workers to provide part of a flow's step handlers (split and multi-language flow execution) 
- [ ] Determine project focus: tasks are so lightweight that with a few extra features they can double as
      a message queue. And we don't just want to duplicate what pgmq does.
      Catbird: is it a cat? is it a bird? It's both.

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
