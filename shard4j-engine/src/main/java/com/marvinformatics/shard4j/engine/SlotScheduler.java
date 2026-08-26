package com.marvinformatics.shard4j.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs one pull loop per drain slot and owns the slots' whole lifecycle: the named worker
 * threads, the stop-pulling flag one slot's failure raises for its siblings, the wait
 * that outlives an interrupt, and surfacing the first failure with the rest suppressed.
 * The drain epilogue -- reconciliation, the barrier -- always sees every slot finished.
 */
final class SlotScheduler {

  /**
   * One slot's failure stops the others from pulling new classes; they finish what they
   * hold. One-shot and never reset, which is why a scheduler belongs to a single drain:
   * {@link ShardLoop} builds a fresh one per lap rather than depending on the flag having
   * stayed down.
   */
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
    AtomicInteger slotNumber = new AtomicInteger();
    ExecutorService pool =
        Executors.newFixedThreadPool(
            slotLoops.size(),
            loop -> new Thread(loop, "shard4j-slot-" + slotNumber.getAndIncrement()));
    try {
      List<Future<?>> slots =
          slotLoops.stream()
              .<Future<?>>map(loop -> pool.submit(stoppingSiblingsOnFailure(loop)))
              .toList();
      awaitAll(pool, slots).surfaceFirst();
    } finally {
      pool.shutdown();
    }
  }

  /**
   * The stop must be raised the moment a slot fails, not when its future is inspected:
   * sibling slots would otherwise keep claiming new classes for the whole remainder of
   * their own drains.
   */
  private Runnable stoppingSiblingsOnFailure(Runnable loop) {
    return () -> {
      try {
        loop.run();
      } catch (RuntimeException | Error e) {
        stopPulling.set(true);
        throw e;
      }
    };
  }

  /**
   * Waits for every slot to finish. An interrupt here must not walk away from live slots:
   * they would keep claiming new classes on non-daemon threads after the engine call
   * already failed, and keep reporting units the shared failure path is about to NACK --
   * so the pulling is stopped, the slots are interrupted out of whatever they hold, and
   * the wait resumes until every slot is actually gone.
   */
  private SlotOutcomes awaitAll(ExecutorService pool, List<Future<?>> slots) {
    List<Throwable> failures = new ArrayList<>();
    boolean interrupted = false;
    for (Future<?> slot : slots) {
      while (true) {
        try {
          slot.get();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
          stopPulling.set(true);
          pool.shutdownNow();
        } catch (ExecutionException e) {
          failures.add(e.getCause());
          break;
        }
      }
    }
    return new SlotOutcomes(interrupted, failures);
  }

  private record SlotOutcomes(boolean interrupted, List<Throwable> failures) {

    /**
     * An interrupt outranks the slots' own failures -- the engine call was cancelled from
     * outside, so that is the story to tell, with the slot failures suppressed behind it.
     * Otherwise the first slot failure wins and the rest ride along suppressed.
     */
    void surfaceFirst() {
      Throwable primary;
      if (interrupted) {
        Thread.currentThread().interrupt();
        primary = new ShardExecutionException("Interrupted while waiting for a drain slot");
      } else if (failures.isEmpty()) {
        return;
      } else {
        primary = failures.get(0);
      }
      Throwable first = primary;
      // Identity-guarded: two slots can surface the same Throwable instance -- the JVM's
      // preallocated OutOfMemoryError -- and self-suppression would mask it entirely.
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
}
