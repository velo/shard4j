package com.example.orders.fixtures;

/** Deliberate, interruption-safe delay, so the ordering fixtures cost measurably different time. */
final class Sleeps {

  private Sleeps() {}

  static void forMillis(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
