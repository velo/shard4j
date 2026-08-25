package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;

/**
 * One pass of the coordinated loop, inside one {@code execute()} call: register the
 * census, ask the coordinator what to run next and drain each class it names, run each
 * grant through a nested Jupiter execution, report every unit as it completes, reconcile,
 * and hold at the barrier. The next pass is the next failsafe execution block in the same
 * Maven run.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
final class ShardLoop {

  private static final System.Logger log = System.getLogger(ShardLoop.class.getName());

  /** Teardown room between deciding to leave and the job actually being killed. */
  private static final Duration DEADLINE_MARGIN = Duration.ofMinutes(3);

  /** How long past a lease expiry the coordinator is given to have acted on it. */
  private static final Duration LEASE_EXPIRY_GRACE = Duration.ofSeconds(30);

  private final ShardConfiguration configuration;
  private final JupiterDelegate jupiter;
  private final CoordinatorGateway gateway;
  private final ExecutionRequest request;

  private final Map<String, Grant> reconciliation = new LinkedHashMap<>();
  private final Map<String, Outcome> outcomes = new LinkedHashMap<>();
  private boolean firstResultPending = true;

  void run(DiscoveredCensus census) {
    gateway.register();
    LivenessKeepalive keepalive = LivenessKeepalive.start(gateway::keepalive);
    Thread abandonOnKill =
        new Thread(
            () -> abandonOutstanding("the shard JVM was terminated mid-pass"),
            "shard4j-abandon-leases");
    Runtime.getRuntime().addShutdownHook(abandonOnKill);
    try {
      claimAndRunUntilDrained(census);
      reconcileOrFail();
      failOnMassAbort();
      holdAtBarrier(keepalive);
    } catch (RuntimeException | Error e) {
      // An abnormal exit must never abandon leases to the TTL: healthy shards would sit
      // at the barrier waiting out earliest_lease_expiry, converting one shard's failure
      // into everyone's slowest path. NACK what is still outstanding, then fail honestly.
      try {
        abandonOutstanding("the shard failed mid-pass: " + e);
      } catch (RuntimeException nackFailure) {
        e.addSuppressed(nackFailure);
      }
      throw e;
    } finally {
      removeHookQuietly(abandonOnKill);
      keepalive.stop();
    }
  }

  /**
   * Returns every still-outstanding lease to the pool, naming the cause. Reached from
   * three places -- the mid-pass failure path, the SIGTERM shutdown hook, and never twice:
   * the snapshot-and-clear makes a second call a no-op, so the hook cannot re-NACK what
   * the failure path already returned.
   */
  private void abandonOutstanding(String cause) {
    List<NackRequest.NackedLease> nacks = new ArrayList<>();
    synchronized (reconciliation) {
      if (reconciliation.isEmpty()) {
        return;
      }
      reconciliation.forEach(
          (unit, grant) ->
              nacks.add(
                  new NackRequest.NackedLease(
                      unit,
                      grant.fence(),
                      "Abandoned on shard "
                          + configuration.shardIndex()
                          + " (pass "
                          + configuration.pass()
                          + "): "
                          + cause
                          + "; returned to the pool")));
      reconciliation.clear();
    }
    gateway.nack(nacks);
  }

  private static void removeHookQuietly(Thread hook) {
    try {
      Runtime.getRuntime().removeShutdownHook(hook);
    } catch (IllegalStateException alreadyShuttingDown) {
      // The hook itself is what runs now; there is nothing to remove.
    }
  }

  /**
   * Pulls until the coordinator answers the open ask with nothing -- the terminal state
   * for this shard. Each ask hands back the class the coordinator wants run next, chosen
   * by its own schedule with the first batch of leases attached; the shard drains that
   * class before asking again. A class the coordinator never names is never entered at
   * all: no nested discovery, no {@code @BeforeAll}, no class initialiser.
   */
  private void claimAndRunUntilDrained(DiscoveredCensus census) {
    while (true) {
      NextClassResponse next = gateway.nextClass();
      if (next.granted().isEmpty()) {
        return;
      }
      // Tracked before the census is consulted: an unknown class name is a coordinator
      // bug, and its grants are NACKed on the way out rather than left to the TTL.
      track(next.granted());
      DiscoveredCensus.ClassUnits entry = census.classNamed(next.className());
      List<Grant> drained = new ArrayList<>(next.granted());
      drained.addAll(drainClass(entry));
      runBatch(next.className(), drained);
    }
  }

  /**
   * Claims the named class until it yields nothing, so everything this shard will run
   * there shares one nested execution -- one class instance, one {@code @BeforeAll} --
   * instead of paying the class setup once per capped claim batch, with other classes
   * interleaved between the payments. Still the pull model: each claim is a fresh ask,
   * and whatever other shards took in between simply is not granted here.
   */
  private List<Grant> drainClass(DiscoveredCensus.ClassUnits entry) {
    List<String> candidates = entry.units().stream().map(ExecutionId::value).toList();
    List<Grant> drained = new ArrayList<>();
    while (true) {
      List<Grant> grants = gateway.claim(entry.className(), candidates);
      if (grants.isEmpty()) {
        return drained;
      }
      track(grants);
      drained.addAll(grants);
    }
  }

  /**
   * Every grant enters reconciliation the moment it arrives, not when its batch runs: a
   * failure between claiming and running -- a transport death mid-drain, a malformed
   * grant -- must NACK what was already leased instead of abandoning it to the TTL.
   */
  private void track(List<Grant> grants) {
    synchronized (reconciliation) {
      grants.forEach(grant -> reconciliation.put(grant.testId(), grant));
    }
  }

  private void runBatch(String className, List<Grant> grants) {
    Map<String, Grant> byUnit = new LinkedHashMap<>();
    grants.forEach(grant -> byUnit.put(grant.testId(), grant));
    // Order is the coordinator's schedule end to end: it chose this class over every
    // other on the open ask, and within the class the grants arrived slowest-first. The
    // nested discovery receives that order intact rather than re-shuffled by a
    // hash-ordered set.
    List<ExecutionId> leased =
        byUnit.keySet().stream().map(unitId -> classRootedLease(className, unitId)).toList();
    TestDescriptor batch =
        jupiter.discoverIds(
            leased,
            request.getConfigurationParameters(),
            request.getOutputDirectoryCreator());
    UnitOutcomeListener listener =
        new UnitOutcomeListener(
            request.getEngineExecutionListener(),
            jupiter.nestedRootId(),
            false,
            Set.copyOf(leased),
            result -> reportCompleted(byUnit, result));
    jupiter.execute(batch, request, listener);
  }

  /**
   * The grant-side half of the wire-id contract the census enforces on the way out: a
   * granted unit the nested discovery could never resolve would be dropped in silence and
   * fall to reconciliation with a message blaming this engine, so a malformed grant fails
   * here naming what the coordinator actually sent.
   *
   * <p>The check is against the class the coordinator named, not merely against the
   * Jupiter root, because the whole batch becomes one nested execution -- one class
   * instance, one {@code @BeforeAll}. A grant from some other class would run there under
   * the wrong setup, and nothing downstream could tell.
   */
  private static ExecutionId classRootedLease(String className, String unitId) {
    if (!unitId.startsWith("[engine:junit-jupiter]/[class:" + className + "]/")) {
      throw new ShardExecutionException(
          "Granted a unit outside the class the coordinator named ("
              + className
              + "), which this engine could never have registered as part of it: "
              + unitId);
    }
    return new ExecutionId(unitId);
  }

  private void reportCompleted(Map<String, Grant> byUnit, UnitResult result) {
    Grant grant = byUnit.get(result.unitId().value());
    boolean firstOnShard = firstResultPending;
    firstResultPending = false;
    gateway.report(grant.fence(), result, firstOnShard);
    synchronized (reconciliation) {
      reconciliation.remove(result.unitId().value());
    }
    outcomes.put(result.unitId().value(), result.outcome());
  }

  /**
   * The pass epilogue. A stale unique-id selector is dropped by the nested discovery in
   * complete silence -- no event, no error, clean exit -- so nothing else in the system can
   * notice that a claimed unit was never run. Anything still leased here is explicitly
   * NACKed back to the pool and the shard fails naming the ids, because a lease this
   * engine cannot explain is a bug in the engine, never a property of the suite.
   */
  private void reconcileOrFail() {
    Map<String, Grant> unexplained;
    synchronized (reconciliation) {
      if (reconciliation.isEmpty()) {
        return;
      }
      unexplained = new LinkedHashMap<>(reconciliation);
      reconciliation.clear();
    }
    List<NackRequest.NackedLease> nacks = new ArrayList<>();
    unexplained.forEach(
        (unit, grant) ->
            nacks.add(
                new NackRequest.NackedLease(
                    unit,
                    grant.fence(),
                    "Leased but never produced a terminal outcome on shard "
                        + configuration.shardIndex()
                        + " (pass "
                        + configuration.pass()
                        + "); returned to the pool")));
    gateway.nack(nacks);
    throw new ShardExecutionException(
        "Shard "
            + configuration.shardIndex()
            + " could not reconcile "
            + unexplained.size()
            + " leased unit(s) to a terminal outcome; they were NACKed back to the pool: "
            + String.join(", ", unexplained.keySet()));
  }

  /**
   * The mass-abort guard: a genuine assumption is a property of one test, so a pass whose
   * entire leased set aborted across more than one class is an environment failure -- a
   * per-JVM latch converting the whole shard's work into aborts would otherwise read as a
   * tidy green that executed nothing.
   */
  private void failOnMassAbort() {
    if (!configuration.allLeasedAbortedIsFailure() || outcomes.isEmpty()) {
      return;
    }
    boolean allAborted = outcomes.values().stream().allMatch(outcome -> outcome == Outcome.ABORTED);
    if (!allAborted) {
      return;
    }
    Set<String> classes = new TreeSet<>();
    outcomes.keySet().forEach(unit -> classes.add(classNameOf(unit)));
    if (classes.size() > 1) {
      throw new ShardExecutionException(
          "Every unit this shard leased in pass "
              + configuration.pass()
              + " ended ABORTED, spanning "
              + classes.size()
              + " classes -- that is an environment failure, not a set of assumptions: "
              + String.join(", ", classes));
    }
  }

  /**
   * Arrival is the pass-completion report; WAIT is polled at the coordinator's cadence,
   * which doubles as proof of life. DONE means stop pulling -- the verdict, not this
   * shard's exit code, decides the run -- and RUN hands control back so the next failsafe
   * execution block can claim from the retry pool.
   */
  private void holdAtBarrier(LivenessKeepalive keepalive) {
    while (true) {
      BarrierResponse response = gateway.barrier(configuration.pass());
      switch (response.action()) {
        case RUN, DONE -> {
          return;
        }
        case WAIT -> {
          int retryAfter = response.retryAfterSeconds() == null ? 5 : response.retryAfterSeconds();
          if (!keepWaiting(retryAfter, response.earliestLeaseExpiry())) {
            log.log(
                System.Logger.Level.INFO,
                "Shard "
                    + configuration.shardIndex()
                    + " cannot outwait the barrier within its own deadline; departing");
            // Quiesced first, not merely signalled: a keepalive claim landing after the
            // departure is proof of life and would rejoin this shard into the quorum,
            // reviving an explicit departure for up to a sweep interval.
            keepalive.stop();
            gateway.depart();
            return;
          }
          sleepSeconds(retryAfter);
        }
      }
    }
  }

  /**
   * The WAIT cap, applied only when the consumer told the engine its kill time: waiting
   * past the point where this shard could still pick the work up burns runner minutes a
   * departure would free.
   */
  private boolean keepWaiting(int retryAfterSeconds, Instant earliestLeaseExpiry) {
    Instant deadline = configuration.deadline();
    if (deadline == null) {
      return true;
    }
    Instant horizon = deadline.minus(DEADLINE_MARGIN);
    if (earliestLeaseExpiry != null && earliestLeaseExpiry.plus(LEASE_EXPIRY_GRACE).isBefore(horizon)) {
      horizon = earliestLeaseExpiry.plus(LEASE_EXPIRY_GRACE);
    }
    return Instant.now().plusSeconds(retryAfterSeconds).isBefore(horizon);
  }

  private static void sleepSeconds(int seconds) {
    try {
      Thread.sleep(Duration.ofSeconds(seconds).toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ShardExecutionException("Interrupted while waiting at the barrier");
    }
  }

  private static String classNameOf(String unitId) {
    int start = unitId.indexOf("[class:") + "[class:".length();
    return unitId.substring(start, unitId.indexOf(']', start));
  }
}
