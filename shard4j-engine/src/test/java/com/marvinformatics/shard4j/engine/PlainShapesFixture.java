package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Executed only through nested delegation by the engine's own tests; the name matches no
 * surefire pattern on purpose. One leaf per outcome the wire knows.
 */
class PlainShapesFixture {

  @Test
  void passes() {}

  @Test
  void fails() {
    fail("deliberate failure");
  }

  @Test
  void abortsInBody() {
    // The dangerous abort shape: the leaf starts, defeating any "did execution begin"
    // check, and then ends aborted.
    assumeTrue(false, "feature flag off");
  }

  @Disabled("not in this environment")
  @Test
  void disabled() {}
}
