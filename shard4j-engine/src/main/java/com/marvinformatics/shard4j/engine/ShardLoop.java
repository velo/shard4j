package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;

/**
 * One pass of the coordinated loop, inside one {@code execute()} call: register the
 * census, sweep the classes claiming incrementally, run each grant through a nested
 * Jupiter execution, report every unit as it completes, reconcile, and hold at the
 * barrier. The next pass is the next failsafe execution block in the same Maven run.
 */
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

  ShardLoop(
      ShardConfiguration configuration,
      JupiterDelegate jupiter,
      CoordinatorGateway gateway,
      ExecutionRequest request) {
    this.configuration = configuration;
    this.jupiter = jupiter;
    this.gateway = gateway;
    this.request = request;
  }

  void run(DiscoveredCensus census) {
    gateway.register();
    LivenessKeepalive keepalive = LivenessKeepalive.start(gateway);
    try {
      claimAndRunUntilDrained(census);
      reconcileOrFail();
      failOnMassAbort();
      holdAtBarrier();
    } finally {
      keepalive.stop();
    }
  }

  /**
   * Sweeps the census class by class, one batched claim per class, until a full sweep
   * grants nothing -- the pull model's terminal state for this shard. An empty grant skips
   * the class outright: no nested discovery, no {@code @BeforeAll}, no class initialiser.
   */
  private void claimAndRunUntilDrained(DiscoveredCensus census) {
    boolean grantedAnything = true;
    while (grantedAnything) {
      grantedAnything = false;
      for (DiscoveredCensus.ClassUnits entry : census.classes()) {
        List<Grant> grants =
            gateway.claim(entry.className(), entry.units().stream().map(ExecutionId::value).toList());
        if (grants.isEmpty()) {
          continue;
        }
        grantedAnything = true;
        runBatch(grants);
      }
    }
  }

  private void runBatch(List<Grant> grants) {
    Map<String, Grant> byUnit = new LinkedHashMap<>();
    grants.forEach(grant -> byUnit.put(grant.testId(), grant));
    reconciliation.putAll(byUnit);
    Set<ExecutionId> leased = new HashSet<>();
    byUnit.keySet().forEach(unit -> leased.add(new ExecutionId(unit)));
    TestDescriptor batch =
        jupiter.discoverIds(
            List.copyOf(leased),
            request.getConfigurationParameters(),
            request.getOutputDirectoryProvider());
    UnitOutcomeListener listener =
        new UnitOutcomeListener(
            request.getEngineExecutionListener(),
            jupiter.nestedRootId(),
            false,
            leased,
            result -> reportCompleted(byUnit, result));
    jupiter.execute(batch, request, listener);
  }

  private void reportCompleted(Map<String, Grant> byUnit, UnitResult result) {
    Grant grant = byUnit.get(result.unitId().value());
    boolean firstOnShard = firstResultPending;
    firstResultPending = false;
    gateway.report(grant.fence(), result, firstOnShard);
    reconciliation.remove(result.unitId().value());
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
    if (reconciliation.isEmpty()) {
      return;
    }
    List<NackRequest.NackedLease> nacks = new ArrayList<>();
    reconciliation.forEach(
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
            + reconciliation.size()
            + " leased unit(s) to a terminal outcome; they were NACKed back to the pool: "
            + String.join(", ", reconciliation.keySet()));
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
  private void holdAtBarrier() {
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
