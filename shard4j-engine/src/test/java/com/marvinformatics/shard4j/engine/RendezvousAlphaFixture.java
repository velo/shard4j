package com.marvinformatics.shard4j.engine;

import org.junit.jupiter.api.Test;

/** Concurrency probe: refuses to finish until {@link RendezvousBetaFixture} has started. */
class RendezvousAlphaFixture {

  @Test
  void meets() throws Exception {
    ConcurrencyProbe.meet("RendezvousAlphaFixture#meets");
  }
}
