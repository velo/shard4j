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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
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
 *
 * <p>{@code shard.concurrency} slots run this pull loop side by side, each draining its
 * own class through its own nested execution -- so {@code @BeforeAll} stays a once-per-
 * class cost while two heavy classes overlap in wall time. The ask-and-drain step is
 * serialised across slots: a class is fully leased before the next open ask, so the
 * coordinator ranks the remaining pool and the second slot receives the next-slowest
 * class, never a slice of the class the first slot is already draining. The barrier is
 * reached only after every slot has finished, so a shard is still exactly one unit of
 * quorum arithmetic no matter how many slots it ran.
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

  private final LeaseLedger ledger = new LeaseLedger();
  private final Map<String, Outcome> outcomes =
      Collections.synchronizedMap(new LinkedHashMap<>());

  /**
   * Serialises ask-and-drain across slots so each ask sees a fully-leased predecessor.
   *
   * <p>The two-lock discipline: this lock is held <em>around</em> multiple gateway calls
   * -- the open ask plus every claim of the drain -- while the gateway's own monitor is
   * only ever held for the duration of a single call. So keepalives and a sibling slot's
   * reports still interleave between a drain's claims, and the pair cannot deadlock:
   * nothing acquires this lock while holding the gateway monitor.
   */
  private final Object dispatchLock = new Object();

  /**
   * One lock per class, holding the one-live-instance-per-class rule across slots. This
   * exists for the expired-lease zombie: the coordinator avoids naming a class the asking
   * shard still holds live leases in, but a lease this shard let expire is no longer
   * live in the coordinator's eyes -- the re-pooled unit can come back through the open
   * ask while the first slot is still running its class. Two live instances of one class
   * in one JVM is a sharper hazard than two different classes, so the second slot waits
   * for the first to leave before entering.
   */
  private final ConcurrentHashMap<String, ReentrantLock> classLocks = new ConcurrentHashMap<>();

  private final SlotScheduler scheduler = new SlotScheduler();

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
   * the ledger's snapshot-and-clear makes a second call a no-op, so the hook cannot
   * re-NACK what the failure path already returned.
   */
  private void abandonOutstanding(String cause) {
    List<NackRequest.NackedLease> nacks =
        Reconciliation.abandoned(
            ledger.drainAll(), configuration.shardIndex(), configuration.pass(), cause);
    if (nacks.isEmpty()) {
      return;
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
   * Runs the pull loop on {@code shard.concurrency} slots and surfaces the first slot
   * failure; the shared failure path then NACKs whatever any slot still holds.
   */
  private void claimAndRunUntilDrained(DiscoveredCensus census) {
    List<Runnable> slotLoops = new ArrayList<>();
    for (int slot = 0; slot < configuration.concurrency(); slot++) {
      Slot drainSlot = new Slot();
      slotLoops.add(() -> drainSlot.pullUntilDrained(census));
    }
    scheduler.runToCompletion(slotLoops);
  }

  /**
   * One drain slot: the pull loop plus the slot-local state it needs. Cold-start
   * exclusion is per slot, not per shard: on a cold JVM every slot's first unit pays the
   * JIT and classloading bill at the same moment, and an unflagged one would record that
   * bill into the duration history driving slowest-first.
   */
  private final class Slot {

    private final AtomicBoolean firstResultPending = new AtomicBoolean(true);

    /**
     * The slot's pull loop, until the coordinator answers the open ask with nothing --
     * the terminal state for this shard. Each ask hands back the class the coordinator
     * wants run next, chosen by its own schedule with the first batch of leases attached;
     * the slot drains that class before running it, and the whole ask-and-drain is one
     * critical section across slots -- so a concurrent ask ranks a pool with the named
     * class fully leased and receives the next-slowest remaining class, not an adjacent
     * slice of this one. A class the coordinator never names is never entered at all: no
     * nested discovery, no {@code @BeforeAll}, no class initialiser.
     */
    void pullUntilDrained(DiscoveredCensus census) {
      while (!scheduler.pullingStopped()) {
        String className;
        List<Grant> drained;
        synchronized (dispatchLock) {
          NextClassResponse next = gateway.nextClass();
          if (next.granted().isEmpty()) {
            return;
          }
          // Tracked before the census is consulted: an unknown class name is a
          // coordinator bug, and its grants are NACKed on the way out rather than left
          // to the TTL.
          ledger.track(next.granted());
          className = next.className();
          drained = new ArrayList<>(next.granted());
          drained.addAll(drainClass(census.classNamed(className)));
        }
        runExclusively(className, drained);
      }
    }

    /**
     * Holds the one-live-instance-per-class rule across slots: entered only once no other
     * slot is running the same class. Waiting happens outside the dispatch lock, so a
     * blocked slot never stalls the other slot's asks.
     *
     * <p>A parked batch is fully leased and nothing refreshes a lease: {@code expiresAt}
     * is fixed at grant, and the keepalive is proof of life for the shard, never a lease
     * extension -- so a parked batch's lease clock runs for the sibling's whole drain
     * plus its own. That is deliberately a sizing rule on the coordinator's
     * {@code leaseTtl} (documented there and in the README) rather than an engine-side
     * refresh: the wire has no refresh call, and adding one would let a wedged slot
     * extend its hold indefinitely, which the TTL exists to bound.
     */
    private void runExclusively(String className, List<Grant> grants) {
      ReentrantLock classLock = classLocks.computeIfAbsent(className, name -> new ReentrantLock());
      try {
        classLock.lockInterruptibly();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ShardExecutionException("Interrupted while waiting to enter " + className);
      }
      try {
        runBatch(className, grants);
      } finally {
        classLock.unlock();
      }
    }

    private void runBatch(String className, List<Grant> grants) {
      Map<String, Grant> byUnit = new LinkedHashMap<>();
      grants.forEach(grant -> byUnit.put(grant.testId(), grant));
      // Order is the coordinator's schedule end to end: it chose this class over every
      // other on the open ask, and within the class the grants arrived slowest-first.
      // The nested discovery receives that order intact rather than re-shuffled by a
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

    private void reportCompleted(Map<String, Grant> byUnit, UnitResult result) {
      Grant grant = byUnit.get(result.unitId().value());
      boolean firstOnShard = firstResultPending.getAndSet(false);
      gateway.report(grant.fence(), result, firstOnShard);
      ledger.explain(result.unitId().value(), grant.fence());
      outcomes.put(result.unitId().value(), result.outcome());
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
      ledger.track(grants);
      drained.addAll(grants);
    }
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

  /**
   * The pass epilogue. A stale unique-id selector is dropped by the nested discovery in
   * complete silence -- no event, no error, clean exit -- so this drain is the only place
   * a claimed-but-never-run unit can be noticed at all. What each unexplained lease
   * means, the wording of its NACK and the failure message are {@link Reconciliation}'s;
   * this method drains, NACKs, and fails when the classification says so.
   */
  private void reconcileOrFail() {
    List<Grant> unexplained = ledger.drainAll();
    if (unexplained.isEmpty()) {
      return;
    }
    Reconciliation reconciliation =
        Reconciliation.classify(unexplained, configuration.shardIndex(), configuration.pass());
    gateway.nack(reconciliation.nacks());
    if (reconciliation.failure() == null) {
      log.log(
          System.Logger.Level.INFO,
          "Shard "
              + configuration.shardIndex()
              + ": every unexplained lease was a cardinality probe; parameter counts"
              + " confirmed");
      return;
    }
    throw new ShardExecutionException(reconciliation.failure());
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
