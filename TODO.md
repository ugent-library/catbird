# Catbird TODO

## API Improvements

- [ ] Implement reflection-based builder API (see REFLECTION_API_DESIGN.md)

## Features

- [ ] Versioning?
- [ ] Fair queueing? (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] Database coordinated scheduling (see SCHEDULING_ADVANCED.md)
- [ ] Allow multi-language/mixed execution of a flow
- [ ] Reuse queue topic bindings implementation for event triggered task execution
- [ ] Task coördination and dynamic tasks (see COORDINATION_PATTERNS.md)
- [ ] Task cancellation and early exit
- [ ] Wait for output long polling
- [ ] Description and other meta fields (part of reflection API)
- [ ] Metrics
- [ ] Schemas for task/flow input/output? (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)
- [ ] OnFail handlers

## Project Management

- [ ] Keep project focused. Queues largely duplicate pgmq.

## Client Implementations

- [ ] Full Typescript client implementation (with workers)
- [ ] Slim client implementations (no workers, facade only) for popular languages

