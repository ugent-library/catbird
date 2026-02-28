# Catbird TODO

## Architecture

- [ ] Implement final flow output model (see FLOW_OUTPUT_MODEL.md)

## API Improvements

- [ ] Review sql function return values and use named constraints

## Features

- [ ] Data archival, cleanup (see DATA_RETENTION.md)
- [ ] Versioning?
- [ ] Fair queueing? (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] Event-triggered task/flow runs via queue bindings MVP (see EVENT_TRIGGER.md)
- [ ] Description and other meta fields
- [ ] Metrics
- [ ] Schemas for task/flow input/output? (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)
- [ ] Relax strict flow reconvergence with explicit, non-ambiguous single-output ownership model (see FLOW_OUTPUT_MODELS.md)
- [ ] Unlogged tasks and flows

## Client Implementations

- [ ] Full Typescript client implementation (with workers)
- [ ] Slim client implementations (no workers, facade only) for popular languages
