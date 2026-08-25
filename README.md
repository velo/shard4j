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
| `shard.concurrency` | no (`1`) | Drain slots per shard: how many classes run at once in this JVM. Above 1, read the in-shard parallelism contract below. |
| `shard.count` | no | Total shards this run launched. A balancing hint only -- it lets the coordinator hold back a fair share of a parameterized method's invocations for shards that have not registered yet, instead of granting them all to whichever shard asked first. Never part of any quorum. |
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

## In-shard parallelism

`shard.concurrency` runs that many pull loops -- drain slots -- side by side in one shard
JVM. Each slot asks the coordinator for a class, drains it completely, and runs it as its
own nested execution, so `@BeforeAll` stays a once-per-class cost while two heavy classes
overlap in wall time. The ask-and-drain step is serialised across slots and a class is
fully leased before the next open ask, so the second slot receives the next-slowest
remaining class -- cross-class slowest-first ordering is preserved, not degraded to
whichever classes are adjacent. Parallelising *within* a class instead would buy almost
nothing on real suites, where most of the duration mass sits in single-leaf classes.

The default is `1`, which is byte-for-byte today's strictly serial behaviour. Nothing on
the coordinator changes either way: a shard is one registration, one keepalive, one
barrier arrival -- it reaches the barrier only after every slot has finished, so it stays
exactly one unit of quorum arithmetic, and it can never be released while a slot still
holds work. A transport death with several slots in flight NACKs everything every slot
still holds.

**The contract, above 1: your test classes must tolerate running concurrently with other
classes in the same JVM.** The engine guarantees at most `shard.concurrency` classes in
flight and never two live instances of the same class, but it cannot see your statics: a
mutable static registry drained per class, a static client reassigned per `@BeforeAll`, a
fixed port, a shared temp directory -- any of these makes concurrent classes unsafe, and
the engine has no way to detect it. Audit for cross-class shared state before opting in;
until then, stay at `1`.

Two more consequences of opting in are part of the same contract. First, threads: above
1, every test runs on a named `shard4j-slot-N` worker thread, never on the thread
failsafe called the engine on -- anything keyed to the main test thread (a thread-local
initialised outside the engine, an AWT/main-thread assumption) moves with it. At `1`
nothing changes. Second, lease sizing: never two live instances of the same class means
a slot granted a class a sibling is already running -- possible whenever a unit is
re-pooled mid-run -- parks with its batch fully leased, and nothing refreshes a lease,
so the coordinator's `leaseTtl` must comfortably exceed roughly **two** full class
drains, not one. An expiry marks the whole shard departed and re-pools the parked batch:
the run stays honest but pays duplicate execution and a confusing red, so size `leaseTtl`
generously before raising `shard.concurrency`.

One compatibility note at `0.1.x`: `ShardConfiguration` gained the `concurrency` record
component mid-signature, which is source-incompatible for anyone calling its constructor
positionally. Configuration keys are the supported surface; the constructor is not.

Orthogonally, Jupiter's own `junit.jupiter.execution.parallel.enabled` passes through to
the nested executions and is tolerated: the engine's outcome accounting is thread-safe
under concurrent events. It parallelises leaves inside one class-drain, which rarely
helps a suite dominated by single-leaf classes, and the same shared-state caveats apply.

## Invocation distribution

A `@ParameterizedTest` method leases as one unit on a cold coordinator, because its
invocations do not exist at discovery time -- a template yields a container and zero
leaves, so the census cannot enumerate them. But the coordinator records per-invocation
durations from the first run onward, and once a method's history carries a complete
breakdown -- every row of a session seen finishing non-failing -- the scheduler hands its
invocations out individually: `#1` to one shard, `#2` to another, each ranked by its own
measured duration. A 5 x 55s method stops being one indivisible 275s block. Methods with
no such history still lease whole -- the same unknowns-first-then-measured shape the
ordering rule already uses, one level down.

What it costs and how it stays honest:

- **Each participating shard pays its own class setup.** Standard JUnit semantics: every
  shard running any invocation instantiates the class and runs `@BeforeAll`/`@AfterAll`
  itself. Spreading is worth it when the rows dwarf the setup, which is exactly the case
  the durations prove.
- **Positions are handed out optimistically and reconciled after.** Invocation ids are
  positional and shift when a `@MethodSource` changes. A handed-out position that no
  longer exists materialises nothing -- JUnit drops it silently -- so the shard's
  reconciliation NACKs it naming the cause (the parameter set changed since it was last
  measured), the run fails loudly, and the coordinator drops the stale position from
  history so the next run expands from the corrected plan.
- **Growth is probed, not assumed away.** Every expanded method also hands out one
  *cardinality probe*: the position just past the recorded plan. Most runs it vanishes
  quietly, confirming the count; when a fixture grew, the probe runs the new row, gets
  measured, and the next position is probed in turn -- so a grown parameter set is
  noticed the run it happens instead of silently never running.
- **Retry and coverage stay per-position.** Each invocation runs the full unit state
  machine: a failed row enters the retry pool alone, any shard may pick it up (paying
  that class setup), and the coverage verdict counts every position individually -- a
  vanished probe leaves the census, everything else must reach a terminal non-failing
  state.
- **History keying stays at method level.** Storage never keys by position; the breakdown
  is a value inside the method-keyed window entry. Distribution acts on positions, storage
  never does.

Set `shard.count` so the coordinator sizes fair shares by the fleet the run launched,
leaving room for shards still booting; without it, spreading still happens but only among
the shards that have already asked. The declared count sizes shares, never strands work:
the cap binds only while another registered, live shard may still ask, so a declared
shard that dies before registering costs nothing -- the last live asker always takes the
remainder.

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
slowest class share, not merely its slowest single test -- and roughly two class shares
once `shard.concurrency` exceeds 1 (see the in-shard parallelism contract).

Retries are additional passes over the session, not in-place re-runs: a failure leaves the
test claimable again in the next pass, on whichever shard asks first, and there are at
most three passes.

The verdict is coverage, never exit codes and never queue emptiness: a session passes only
when every registered test reached a terminal non-failing state. If every shard departs
while tests remain unfinished, the session is incomplete, and incomplete is not green.

## Licence

Apache-2.0. Copyright 2026 Marvin Froeder.
