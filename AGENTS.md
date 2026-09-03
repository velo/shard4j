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
particular ends up on every consumer's test classpath. Raising it to 21 or 25 would gate
adoption of a test-sharding library on a JDK the adopting project may not run yet -- the
cost lands on the consumer, not on us, which is exactly the wrong place for it.

**17 is inherited, and no longer negotiable.** JUnit Platform 6 declares
`Require-Capability: osgi.ee=JavaSE version=17`, so the engine cannot target lower whatever
its own sources look like. It was self-imposed while the engine sat on Platform 1.14, which
was a Java 8 artifact -- as surefire and failsafe still are -- and that is worth remembering
only so nobody re-opens the question: lowering the floor is now impossible, not merely a
rewrite.

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

## Testing

Integration tests running the server on Docker are the cornerstone of automated testing
here. Unit tests cover pure logic. Mock only as a last resort, where an integration test
is genuinely impossible -- never for convenience.

`shard4j-protocol` is pure logic with no I/O, which is why its 258 tests need neither a
server nor a mock. That is the exception its nature earns, not the pattern to copy: the
coordinator must be tested as a real running server in Docker.

Assertions are AssertJ. `org.junit.jupiter.api.Assertions` is not a second house style --
it is what the OpenRewrite profile below converts away from.

## Mechanised best practice

```
mvn -Popenrewrite verify    # rewrites sources in place, then review the diff
mvn verify                  # prove the rewrite still compiles and passes
```

Off by default, like `release`: the everyday build stays fast and offline.

**Run it until it stops changing things.** The AssertJ Refaster rules rewrite in steps --
`compareTo(x) isLessThan(0)` becomes `isLessThan(x)` only once an earlier rule has turned
the assertion into the shape the later rule matches. Two passes reached a fixpoint the last
time; one pass is not proof of anything.

Two constraints are worth knowing before editing the profile:

- **pom.xml is excluded from every execution.** The recipes would otherwise "fix" the two
  things this build pins on purpose: the per-module `release` indirection and the JUnit
  Platform floor. Dependency and plugin versions move by human decision, or by Dependabot.
- **The `test-frameworks` execution must not reach `src/main/java`.** `Assertj` chains in
  `JUnit5BestPractices`, which chains in the JUnit Platform upgrade; `shard4j-engine`
  *implements* that Platform SPI in its main sources, so a blanket run rewrites the engine
  against an API its own classpath may not carry. Everything in the second execution is
  safe anywhere, which is why only the first one is confined.

### What is wired in, and what was tried and rejected

| Recipe | | Why |
|---|---|---|
| `testing.assertj.Assertj` + four `junit5` recipes | in | AssertJ and Jupiter hygiene, test sources only |
| `migrate.UpgradeToJava17` / `...25` | in | tracks the baselines table via `rewrite.java.recipe` |
| `spring.boot4.*` | in | coordinator only -- the one module that has heard of Spring |
| `staticanalysis.CommonStaticAnalysis` | in | import order, diamonds, method refs, `EqualsAvoidsNull` |
| `security.JavaSecurityBestPractices` | in | finds nothing today; kept as a standing guard |
| `migrate.lombok.LombokBestPractices` | **out** | see below |
| `maven.BestPractices` | **out** | see below |

`LombokBestPractices` is not wired in because two of its rules are actively wrong here:

- `UseRequiredArgsConstructor` replaced `CoordinatorCore`'s hand-written constructor with
  `@RequiredArgsConstructor(onConstructor_ = {@Builder})`, which **deletes
  `CoordinatorCore.builder()`** -- the build stops compiling. It also deletes the comment
  explaining why that constructor is hand-written, and reintroduces exactly the hazard the
  comment warns about: `@RequiredArgsConstructor` takes its parameter order from field
  declaration order, so a field reorder silently reorders the constructor.
- `UseLombokGetter` put `@Getter` on a **record component**, where Lombok generates
  nothing. That one compiles, which is worse: the interface's `default`
  `getOutputDirectoryCreator()` -- which throws -- would have taken over at runtime.

Three of its constructor rewrites were pure boilerplate removal and were taken by hand
(`ShardLoop`, `LivenessKeepalive`, `Session.UnitState`). A blanket `@Getter`/`@Setter` pass
would also fight the `@Accessors(fluent = true)` convention below.

`maven.BestPractices` is the case the pom.xml exclusion was written for, and running it
confirmed the rule rather than the exception:

- `SortDependencies` alphabetises dependencies, which tears every dependency away from the
  comment above it explaining why it is there.
- `RemoveRedundantDependencyVersions` stripped `${lombok.version}` from the coordinator,
  silently handing Lombok's version to Spring Boot's BOM instead of the property this
  repository pins on purpose.

## Dependency updates

Dependabot opens the PRs; `.github/workflows/auto-merge-dependabot.yml` approves each one
and queues an auto-merge. Blanket auto-merge is only defensible because `build.yml` is the
gate -- module boundary, forbiddenapis, the coordinated failsafe profile against a live
coordinator, and the container smoke test all run before anything merges.

Three settings live in the repository rather than the tree. All three are set, and the
workflow is inert or red without them:

- **"Allow auto-merge"** on the repository, or the merge step fails outright.
- **`main` requires the three `build.yml` checks.** Without them `--auto` degrades to
  merging on the spot rather than waiting for green, which removes the safety argument the
  whole arrangement rests on.
- **"Allow GitHub Actions to create and approve pull requests"** (Settings -> Actions ->
  General), or the approve step fails with "GitHub Actions is not permitted to approve pull
  requests". `github-actions[bot]` approving `dependabot[bot]` is two distinct identities,
  which is what makes it permissible; a token approving its own PR still would not be.

**`github_actions` updates are approved but not queued, and that is not a bug to fix.**
They edit files under `.github/workflows/`, and GitHub refuses any `GITHUB_TOKEN` merge that
does -- *"refusing to allow a GitHub App to create or update workflow ... without
`workflows` permission"*. That scope exists only on a PAT, and this repository does not use
one; `build.yml` makes the same choice for the GHCR push. Merge those by hand. Reaching for
a PAT to close the gap would put a long-lived credential with write access to the workflows
themselves into repository secrets, which buys a little convenience for a lot of blast
radius.

`org.junit:junit-bom` and `io.github.openfeign:feign-bom` are ignored for major versions,
and the JUnit 5 to 6 move is why the rule exists rather than an argument against it. That
bump changed the engine's Java floor, dropped `ConfigurationParameters.size()`, added a
parameter to the one Platform method this engine calls, and put a new jar on every
consumer's test classpath. Every one of those needed a human. A minor is a decision; a
major is a decision someone has to make on purpose.

## Repository hygiene

- **No secrets, ever.** Values arrive only through `COORDINATOR_SECRETS` and
  `SHARD_COORDINATOR_SECRET`. No example value that could be mistaken for a real one.
- **No deployment manifests of any kind**, and no `deploy/` directory. A `Dockerfile` and a
  `docker-compose.yml` for local development would be in scope; anything naming a
  *destination* is not.
- **No hostnames, registries, account ids, cluster names, namespaces or organisation names**
  in code, tests, fixtures or comments. shard4j is a product; the projects that run it are
  consumers, and nothing identifying any of them belongs here. There is deliberately no
  in-repo check for this: a script that greps for such strings has to contain them, which
  is itself the leak. Enforce the rule in review, or with a private pre-commit hook in your
  own clone's `.git/hooks/` -- hooks are untracked and ship nowhere.
- Example tenants are `example/orders-service` and friends; example packages are
  `com.example.*`.

## Conventions

- Lombok (`provided` scope, so it never enters a shipped dependency tree) supplies
  constructors (`@RequiredArgsConstructor`), builders (`@Builder`), loggers (`@Slf4j`),
  fluent accessors (`@Getter` with `@Accessors(fluent = true)`) and static-only classes
  (`@UtilityClass`) where they replace pure boilerplate. A constructor that validates,
  normalises or derives state stays hand-written.
- Test methods -- unit and integration alike -- are named `given_when_then`, camelCase
  inside each clause: `givenTornTail_whenAppendingAfterReopen_thenNewRecordSurvivesReplay`.
- Files end with a newline. No trailing whitespace on blank lines.
- Comments explain *why*, not what the next line does.
- Use imports; never a fully-qualified class name in code.
- Single-line commit messages, no co-author trailers, signed (`git commit -s -S`).
