package com.marvinformatics.shard4j.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs one pull loop per drain slot and owns the slots' whole lifecycle: the named worker
 * threads, the stop-pulling flag one slot's failure raises for its siblings, the join
 * that outlives an interrupt, and surfacing the first failure with the rest suppressed.
 * The pass epilogue -- reconciliation, the barrier -- always sees every slot finished.
 */
final class SlotScheduler {

  /** One slot's failure stops the others from pulling new classes; they finish what they hold. */
  private final AtomicBoolean stopPulling = new AtomicBoolean();

  boolean pullingStopped() {
    return stopPulling.get();
  }

  /**
   * Runs every slot loop to completion. A single slot runs inline, deliberately: it keeps
   * {@code shard.concurrency=1} byte-for-byte serial behaviour, including thread identity
   * -- the pull loop stays on the engine's calling thread, so consumers' thread-confined
   * state and stack traces are exactly what the serial engine produced. Do not
   * "simplify" the branch away into the worker path.
   */
  void runToCompletion(List<Runnable> slotLoops) {
    if (slotLoops.size() == 1) {
      slotLoops.get(0).run();
      return;
    }
    List<Thread> workers = new ArrayList<>();
    List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
    for (int slot = 0; slot < slotLoops.size(); slot++) {
      Runnable loop = slotLoops.get(slot);
      Thread worker =
          new Thread(
              () -> {
                try {
                  loop.run();
                } catch (RuntimeException | Error e) {
                  stopPulling.set(true);
                  failures.add(e);
                }
              },
              "shard4j-slot-" + slot);
      worker.start();
      workers.add(worker);
    }
    boolean interrupted = joinAll(workers);
    surfaceFirst(interrupted, failures);
  }

  /**
   * Waits for every slot to finish. An interrupt here must not walk away from live slots:
   * they would keep claiming new classes on non-daemon threads after the engine call
   * already failed, and keep reporting units the shared failure path is about to NACK --
   * so the pulling is stopped, the slots are interrupted out of whatever they hold, and
   * the wait resumes until every slot is actually gone.
   */
  private boolean joinAll(List<Thread> workers) {
    boolean interrupted = false;
    for (Thread worker : workers) {
      while (true) {
        try {
          worker.join();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
          stopPulling.set(true);
          workers.forEach(Thread::interrupt);
        }
      }
    }
    return interrupted;
  }

  private static void surfaceFirst(boolean interrupted, List<Throwable> failures) {
    Throwable primary;
    if (interrupted) {
      Thread.currentThread().interrupt();
      primary = new ShardExecutionException("Interrupted while waiting for a drain slot");
    } else if (failures.isEmpty()) {
      return;
    } else {
      primary = failures.get(0);
    }
    // Identity-guarded: two slots can surface the same Throwable instance -- the JVM's
    // preallocated OutOfMemoryError -- and self-suppression would mask it entirely.
    Throwable first = primary;
    failures.stream().filter(failure -> failure != first).forEach(first::addSuppressed);
    if (primary instanceof RuntimeException runtime) {
      throw runtime;
    }
    if (primary instanceof Error error) {
      throw error;
    }
    throw new ShardExecutionException("A drain slot failed", primary);
  }
}
