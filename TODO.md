# Catbird TODO

- [ ] Versioning?
- [ ] Fair queueing? (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] Database coordinated scheduling (see SCHEDULING_ADVANCED.md)
- [ ] Allow multi-language/mixed execution of a flow
- [ ] Use a generics backed type safe builder (see GENERIC_API_DESIGN.md).
      Alternatively use a caching reflection based approach (see REFLECTION_API_DESIGN.md). Overhead is acceptable in go 1.21+.
- [ ] Keep project focused. Queues largely duplicate pgmq.
- [ ] Reuse queue topic bindings implementation for event triggered task execution. 
- [ ] Task coördination and dynamic tasks (see COORDINATION_PATTERNS.md)
- [ ] Task cancellation and early exit
- [ ] Wait for output long polling
- [ ] Description and other meta fields
- [ ] Metrics
- [ ] Schemas for task/flow input/output? (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)
- [ ] OnFail handlers
- [ ] Full Typescript client implementation (with workers)
- [ ] Slim client implementations (no workers, facade only) for popular languages
