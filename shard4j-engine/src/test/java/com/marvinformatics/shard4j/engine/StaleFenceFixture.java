package com.marvinformatics.shard4j.engine;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Holds its single unit open until {@link StaleFenceLedgerTest}'s scripted gateway releases it. */
class StaleFenceFixture {

  static volatile CountDownLatch release = new CountDownLatch(1);

  @Test
  void unit() throws Exception {
    if (!release.await(20, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Never released; the scripted interleaving broke");
    }
  }
}
