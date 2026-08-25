package com.marvinformatics.shard4j.engine;

import org.junit.jupiter.api.Test;

/** Concurrency probe: refuses to finish until {@link RendezvousAlphaFixture} has started. */
class RendezvousBetaFixture {

  @Test
  void meets() throws Exception {
    ConcurrencyProbe.meet("RendezvousBetaFixture#meets");
  }
}
