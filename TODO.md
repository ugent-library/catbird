# Catbird TODO

## API Improvements

### Naming & API Cleanup

- [ ] Review sql function return values

## Features

- [ ] Versioning?
- [ ] Fair queueing? (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] Reuse queue topic bindings implementation for event triggered task execution?
- [ ] Task coördination and dynamic tasks (see COORDINATION_PATTERNS.md)
- [ ] Task cancellation and early exit
- [ ] Description and other meta fields 
- [ ] Metrics
- [ ] Schemas for task/flow input/output? (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)
- [ ] OnFail handlers
- [ ] Unlogged tasks and flows

## Client Implementations

- [ ] Full Typescript client implementation (with workers)
- [ ] Slim client implementations (no workers, facade only) for popular languages
