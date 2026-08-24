# shard4j

Coordinator-driven, duration-balanced sharding for JUnit Platform test suites.

A single-instance HTTP service hands integration tests to CI shards one class-batch at a
time, slowest-first by measured duration, and answers one question at the end of a run:
did every registered test reach a terminal non-failing state? Balancing needs durations,
durations need a store, and correctness under pull-based distribution needs a central
verdict. That is the whole service.

Static hash sharding is blind to duration. On the suite this was built for, a per-class
span of 1.3s to 702s produced a 9-22 minute spread across shards -- most of the runner
time spent waiting on one unlucky shard.

## Modules

| Module | Java | What it is |
|---|---|---|
| `shard4j-protocol` | 17 | Wire records, the execution-id grammar, the history-key derivation. Zero runtime dependencies. |
| `shard4j-coordinator` | 25 | The service. |
| `shard4j-engine` | 17 | The JUnit Platform `TestEngine` and its coordinator client. Never depends on the coordinator. |
| `shard4j-example` | 17 | A project that has never heard of shard4j, used as the boundary acceptance test. Not published. |

Coordinates are `com.marvinformatics:shard4j-{protocol,coordinator,engine}`, one version
across all three, released from one reactor. That is why there is no protocol version field
on the wire: a contract change breaks compilation, at build time, rather than being
renegotiated at runtime.

## Build

```
mvn verify
```

A JDK and Maven, and nothing else. No container, no cluster, no account.

## Status

Skeleton. The module layout, the Java baselines, the enforced module boundary and CI are
in place; the coordinator and engine are not implemented yet.

## Deployment

**This repository contains no deployment configuration, by design.** There is no Helm
chart, no Kubernetes manifest, no Terraform module and no `deploy/` directory, and none
will be added. The moment one exists, someone parameterises it with a real hostname. The
coordinator needs a TCP port, one writable directory and a wall clock; where it runs is
entirely a deployer's business, and every value it needs arrives as an environment
variable with no default.

For the same reason there are no secrets here, and no example value that could be mistaken
for a real one.

## Security posture

The shared secret is a CI-scheduling credential. Its entire blast radius is test
scheduling for one instance: it grants nothing on any cluster, in any cloud, or on any
repository. A holder of the secret plus a live session id can inject results, poison timing
history and drain a session's queue; they cannot overwrite a recorded result or turn a
drained session green.

v1 is single-tenant per instance -- one instance is one trust domain. **Do not add a
client-supplied tenant field to get multi-tenancy**: that converts one leaked secret into
cross-tenant write access. A second tenant gets a second instance.

## Design

There is no public specification document yet; the shape of the system is this.

A run is a session. Each CI shard registers with the coordinator, then loops: claim a
class-batch, run it, report each result, claim again, and depart when the coordinator has
nothing left to hand out. The coordinator answers each claim with the slowest
not-yet-claimed classes first, using durations measured on earlier runs; a test with no
history is ordered by a hash of its identity, so the schedule stays deterministic without
being alphabetical. Every claim is a lease with an expiry and a fence, so a shard that
stalls or dies loses its work back to the queue instead of taking the run down with it.

Retries are additional passes over the session, not in-place re-runs: a failure leaves the
test claimable again in the next pass, on whichever shard asks first, and there are at
most three passes.

The verdict is coverage, never exit codes and never queue emptiness: a session passes only
when every registered test reached a terminal non-failing state. If every shard departs
while tests remain unfinished, the session is incomplete, and incomplete is not green.

## Licence

Apache-2.0. Copyright 2026 Marvin Froeder.
