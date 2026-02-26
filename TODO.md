# Catbird TODO

## API Improvements

### Naming & API Cleanup

- [ ] Review sql function return values and use named constraints

## Features

- [ ] Data archival, cleanup (see DATA_RETENTION.md)
- [ ] Versioning?
- [ ] Fair queueing? (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] Event-triggered task/flow runs via queue bindings MVP (see EVENT_TRIGGER.md)
- [ ] Flow coördination and dynamic tasks (see REMAINING_PATTERNS.md)
- [ ] Cancellation (see CANCELLATION.md)
- [ ] Early exit (see EARLY_EXIT.md)
- [ ] Description and other meta fields
- [ ] Metrics
- [ ] Schemas for task/flow input/output? (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)
- [ ] Unlogged tasks and flows

## Client Implementations

- [ ] Full Typescript client implementation (with workers)
- [ ] Slim client implementations (no workers, facade only) for popular languages
