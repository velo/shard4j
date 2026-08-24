# AGENTS.md

Instructions for agents and humans working in this repository.

## Build

```
mvn verify                 # the whole reactor, including the example module's failsafe run
mvn -q -pl shard4j-engine -am verify
```

One JDK builds everything: **JDK 25 or newer is required to build**, and the enforcer
fails the build with that message if it is older. There is no toolchains file -- see the
Java baselines below for why none is needed.

Offline, no cloud account, no cluster, no credentials. If a build step ever needs one of
those, that step is wrong.

## Java baselines, and why they differ

| Module | `maven.compiler.release` |
|---|---|
| `shard4j-protocol` | 17 |
| `shard4j-engine` | 17 |
| `shard4j-example` | 17 |
| `shard4j-coordinator` | 25 |

The two published libraries land on **someone else's classpath**. `shard4j-engine` in
particular ends up on every consumer's test classpath, and it pins JUnit Platform 1.13,
whose own floor is 17. Raising the engine to 21 or 25 would gate adoption of a test-sharding
library on a JDK the adopting project may not run yet -- the cost lands on the consumer,
not on us, which is exactly the wrong place for it.

The coordinator is a **deployed service**: it is nobody's dependency, its runtime is chosen
by whoever deploys it, and it is free to sit on the current LTS.

The mechanism is a per-module `maven.compiler.release` property rather than Maven
toolchains. Toolchains would need every contributor and the CI runner to have two JDKs
installed and a `~/.m2/toolchains.xml` to match, which is real friction for a repository
whose contributor bar is "clone, build and test offline in under five minutes". `release`
gives the same guarantee -- the compiler refuses APIs newer than the target -- from a single
JDK and a single property.

## The module boundary

```
        shard4j-protocol          zero runtime dependencies
             ^        ^
   coordinator        engine      engine MUST NOT reach coordinator, directly or transitively
```

**This is the project's central invariant, and it is enforced by the build, not by review.**

Why it matters: `shard4j-engine` is shipped onto every consumer's test classpath. If the
coordinator's server framework can arrive with it, adopting a test-sharding library means
adopting a web framework, and the engine stops being something a project that has never
heard of us can use. The failure mode is not dramatic -- it arrives quietly, as "just move
this helper up into protocol".

Four checks, all of which fail `mvn verify`:

1. **`shard4j-engine` resolves no coordinator artifact and no Spring artifact**, at any
   scope, transitively (`maven-enforcer-plugin`, `bannedDependencies`).
2. **`shard4j-engine`'s shipped runtime tree is an allow-list** -- `shard4j-protocol` +
   JUnit Platform + Feign + one JSON codec, and nothing else. A new runtime dependency has
   to be a deliberate edit to that rule.
3. **`shard4j-protocol` has an empty runtime dependency tree.** Every resolved dependency
   must be test- or provided-scoped. It holds wire records and identity functions only:
   never a config key, never a hostname, never a storage path, never a default.
4. **`shard4j-engine` references nothing in `org.junit.platform.launcher.core`**
   (`forbiddenapis`, over bytecode). A dependency-tree check cannot catch this: that package
   ships inside `junit-platform-launcher`, which is legitimately present, so the tree stays
   green while the engine quietly re-acquires the launcher orchestrators -- three
   `@API(INTERNAL)` types instead of one, plus a classpath-only constraint, since the package
   is qualified-exported. The engine delegates through `TestEngine.discover(request, uniqueId)`
   plus the 5-arg `ExecutionRequest.create`, and nothing else.

`shard4j-example` carries the same ban on its own test classpath, which is where the
boundary is observable from outside.

To see check 1 fire, add a Spring dependency to `shard4j-engine/pom.xml` and run
`mvn -pl shard4j-engine verify`.

## HTTP client

Feign, in `shard4j-engine`. It is small and framework-agnostic, so it does not compromise
the footprint rule, and its `Retryer` / `ErrorDecoder` vocabulary is the one the retry-budget
design is already written in. It is on the enforcer's allow-list deliberately. The
coordinator's inbound side is Spring Boot; Feign is client-side only.

## `module-info`

**There is none, deliberately.** Each published jar carries an `Automatic-Module-Name`
instead:

- The engine is loaded from the **test classpath** by Surefire/Failsafe, which is not a
  module path, so a descriptor would buy nothing at the one place the engine actually runs.
- A descriptor would make the launcher-orchestrator route a hard compile error rather than
  the deliberate design choice check 4 already enforces -- and it would do so by accident, in
  a way that reads as a packaging constraint rather than as the API decision it is.
- `shard4j-protocol` must stay dependency-free and trivially consumable; a descriptor adds a
  maintenance surface with no consumer asking for it.

`Automatic-Module-Name` keeps a stable module name available for anyone who does put these
jars on a module path, at no cost. Revisit only if a real consumer needs true JPMS.

## Repository hygiene -- enforced in CI

- **No secrets, ever.** Values arrive only through `COORDINATOR_SECRETS` and
  `SHARD_COORDINATOR_SECRET`. No example value that could be mistaken for a real one.
- **No deployment manifests of any kind**, and no `deploy/` directory. A `Dockerfile` and a
  `docker-compose.yml` for local development would be in scope; anything naming a
  *destination* is not.
- **No hostnames, registries, account ids, cluster names, namespaces or organisation names**
  in code, tests, fixtures or comments. `scripts/check-hygiene.sh` greps the tree for these
  and fails the `build` job. The single allow-listed exception is the README's link to the
  design issue and its "first known user" line.
- Example tenants are `example/orders-service` and friends; example packages are
  `com.example.*`.

## Conventions

- Files end with a newline. No trailing whitespace on blank lines.
- Comments explain *why*, not what the next line does.
- Use imports; never a fully-qualified class name in code.
- Single-line commit messages, no co-author trailers, signed (`git commit -s -S`).
