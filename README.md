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
| `shard4j-protocol` | 17 | Wire records, the identity value types, the no-history ordering hash. Zero runtime dependencies. |
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

Coordinator and engine are implemented and tested against each other -- the
`shard4j-example` module runs the full loop (census, claims, execution, results, barrier,
retry rebalance) across simulated shards against the real coordinator in a container, and
carries a working `coordinated` failsafe profile.

## The engine

`shard4j-engine` is a JUnit Platform `TestEngine`, `ServiceLoader`-registered, that lands
on a consumer's **test classpath** and stays completely inert until configured: with
`shard.enabled` absent or false its discovery is empty, so Jupiter behaves exactly as if
the engine were not there. In the CI profile, failsafe's
`<excludeJUnit5Engines>junit-jupiter</excludeJUnit5Engines>` hands the suite to this
engine instead, which delegates discovery and execution back to Jupiter while pulling
work from the coordinator one class-batch at a time. `shard4j-example/pom.xml`'s
`coordinated` profile is the complete, working integration: three `integration-test`
executions (`main`, `retry1`, `retry2`), each with its own summary file and reports
directory, and one `verify` per pass -- per pass, because an execution that claimed
nothing writes no summary file at all, and a single aggregating verify would fail on the
missing file exactly on the healthy early-release path.

A shard's exit code is not the run's verdict; the coordinator's coverage verdict is. A
test that failed in `main` and passed in `retry1` still leaves a failure in `main`'s
summary, so a shard job can exit non-zero on a session the coordinator judges green.
The one safe way to wire that up is a **non-gating shard job**: every shard job reports
its own exit honestly, and the pipeline gates on a final step that reads the
coordinator's verdict for the session. Do not reconcile the two with failsafe's
`testFailureIgnore`, under this engine or next to it: it does not merely forgive a
retried failure, it discards every failure the summary carries -- engine errors,
mass-abort failures, reconciliation failures, and the `@AfterAll` that throws after its
class's tests all passed, which no per-unit record can see because units are reported
before the container finishes. That last shape leaves the verdict all-PASSED with the
summary failure as the only surviving signal, and `testFailureIgnore` deletes exactly
that signal. This is a rule, not a preference.

Configuration is read from JUnit configuration parameters (which the launcher backs with
system properties) first, then environment variables (`shard.foo.bar` maps to
`SHARD_FOO_BAR`):

| Key | Required | Meaning |
|---|---|---|
| `shard.enabled` | no (`false`) | Master switch; absent or false means completely inert. |
| `shard.coordinator.url` | yes | Base URL. No default, ever. |
| `SHARD_COORDINATOR_SECRET` | yes | **Environment variable only.** A value supplied as a system property is refused: properties appear in `ps` output and argLine echoes. |
| `shard.session.id` | yes | Run-scoped id minted upstream of the shards, so every shard reads one value and a partial re-run rejoins. |
| `shard.index` | yes | 0-based shard index. |
| `shard.pass` | yes | `main` \| `retry1` \| `retry2`, one per execution block. |
| `shard.attempt` | no (`1`) | Monotonic re-run counter; a higher value voids the previous attempt's leases. |
| `shard.metadata.*` | no | Forwarded verbatim; the only seam CI-vendor vocabulary may pass through. |
| `shard.coordinator.retry.budget` | no (`5m`) | Transport retry window; must exceed the coordinator deployment's restart time. |
| `shard.deadline` | no | Absolute job-kill instant (ISO-8601); enables early self-release at the barrier. |
| `shard.abort.all-leased-is-failure` | no (`true`) | Fail the shard when every leased unit across more than one class aborted. |

On GitHub Actions the mapping is: a setup job mints `shard.session.id` with `uuidgen` as a
job output, the matrix supplies `shard.index`, and the repository secret is exported as
`SHARD_COORDINATOR_SECRET` via `env:` on the step. On plain shell it is the
`SHARD_COORDINATOR_SECRET=... mvn -Pcoordinated verify -Dshard...` invocation shown in the
example's profile comment. Neither is canonical; any CI that can mint one UUID upstream
and number its shards qualifies.

Two liveness rules bind the two sides together. A shard holding a lease is trusted until
the lease expires; a shard holding **no** lease that stays silent for three barrier-poll
intervals is presumed dead and dropped from barrier quorums. The engine honours the
second rule with a background keepalive that pings an empty claim every five seconds for
the whole of `execute()`, covering the gaps a real suite has -- a slow `@AfterAll`
between classes, a long class setup before the first result -- where it would otherwise
be silent while holding nothing. That coverage stops at the edges of `execute()`: between
the per-pass executions the shard is a JVM tearing down and a fresh fork spinning up --
classpath scan, discovery of the next pass -- holding no lease and sending nothing, and on
a large consumer classpath that gap can outlast the coordinator's 15-second presumed-death
tolerance. The consequence is a premature presumed death: a degraded rebalance that
self-heals on the shard's next call, so the outcome is red or correct, never a false
green. The engine cannot close this gap, because the shard genuinely is not running
during it.

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

`COORDINATOR_SECRETS` is split on commas so rotation can run two values side by side --
which means **a secret value must not contain a comma**: it would silently become two
wrong values, and neither would ever match. Generate secrets from a comma-free alphabet
(hex or base64url). The coordinator refuses to start when the split produces a blank
entry, the tell-tale of a stray comma, but it cannot detect a comma inside a value.

v1 is single-tenant per instance -- one instance is one trust domain. **Do not add a
client-supplied tenant field to get multi-tenancy**: that converts one leaked secret into
cross-tenant write access. A second tenant gets a second instance.

## Design

There is no public specification document yet; the shape of the system is this.

A run is a session. Each CI shard registers with the coordinator, then loops: ask what
to run next, drain the class the coordinator names, report each result, and ask again
until the coordinator has nothing left to hand out. The choice of class is the
coordinator's, made from durations measured on earlier runs: the class holding the
slowest remaining test is named first, and the answer carries that class's first batch of
leases so a named class is never an empty promise. A test with no history runs before
every measured one, ordered by a hash of its identity, so the schedule stays
deterministic without being alphabetical. Every grant is a lease with an expiry and a
fence, so a shard that stalls or dies loses its work back to the queue instead of taking
the run down with it.
Draining a class grants all of its leases up front, so the lease TTL must cover a shard's
slowest class share, not merely its slowest single test.

Retries are additional passes over the session, not in-place re-runs: a failure leaves the
test claimable again in the next pass, on whichever shard asks first, and there are at
most three passes.

The verdict is coverage, never exit codes and never queue emptiness: a session passes only
when every registered test reached a terminal non-failing state. If every shard departs
while tests remain unfinished, the session is incomplete, and incomplete is not green.

## Licence

Apache-2.0. Copyright 2026 Marvin Froeder.
