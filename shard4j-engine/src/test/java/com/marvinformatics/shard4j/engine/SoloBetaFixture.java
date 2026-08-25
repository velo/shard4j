package com.marvinformatics.shard4j.engine;

import org.junit.jupiter.api.Test;

/** Window probe with no cross-class coupling, for the strictly-serial assertions. */
class SoloBetaFixture {

  @Test
  void occupies() throws InterruptedException {
    ConcurrencyProbe.occupy("SoloBetaFixture#occupies");
  }
}
