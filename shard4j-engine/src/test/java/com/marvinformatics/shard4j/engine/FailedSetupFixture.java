package com.marvinformatics.shard4j.engine;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The third setup shape: {@code @BeforeAll} throws rather than aborting, so the class
 * container finishes FAILED having emitted no events at all for its leaves. It is the only
 * event from which those units can be explained -- and, when the coordinator still owes
 * them a retry, the only event that can carry the downgrade to the launcher.
 */
class FailedSetupFixture {

  @BeforeAll
  static void brokenSetup() {
    throw new IllegalStateException("the fixture database is unreachable");
  }

  @Test
  void first() {}

  @Test
  void second() {}
}
