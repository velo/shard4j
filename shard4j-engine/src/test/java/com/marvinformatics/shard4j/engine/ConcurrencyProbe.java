package com.marvinformatics.shard4j.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import lombok.experimental.UtilityClass;

/**
 * Shared journal for the concurrency fixtures: each probe records the wall-clock window
 * of its own execution, and the rendezvous variant additionally refuses to finish until
 * another probe has started -- so a run in which the two windows overlap is proven
 * concurrent by construction, and a strictly serial engine times the rendezvous out.
 */
@UtilityClass
class ConcurrencyProbe {

  record Window(String label, long startNanos, long endNanos) {}

  final List<Window> WINDOWS = Collections.synchronizedList(new ArrayList<>());

  private CyclicBarrier rendezvous;

  void reset(int parties) {
    WINDOWS.clear();
    rendezvous = parties > 0 ? new CyclicBarrier(parties) : null;
  }

  /** Waits for the other party; a serial engine leaves this waiting until the timeout. */
  void meet(String label) throws Exception {
    long start = System.nanoTime();
    rendezvous.await(20, TimeUnit.SECONDS);
    WINDOWS.add(new Window(label, start, System.nanoTime()));
  }

  /** Occupies a measurable window without any cross-class coupling. */
  void occupy(String label) throws InterruptedException {
    long start = System.nanoTime();
    Thread.sleep(200);
    WINDOWS.add(new Window(label, start, System.nanoTime()));
  }

  boolean overlapped(String labelA, String labelB) {
    Window a = windowOf(labelA);
    Window b = windowOf(labelB);
    return Math.max(a.startNanos(), b.startNanos()) < Math.min(a.endNanos(), b.endNanos());
  }

  private Window windowOf(String label) {
    synchronized (WINDOWS) {
      return WINDOWS.stream()
          .filter(window -> window.label().equals(label))
          .findFirst()
          .orElseThrow(() -> new AssertionError("No recorded window for " + label));
    }
  }
}
