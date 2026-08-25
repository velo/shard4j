package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Four rows, one of which fails its first execution in this JVM and passes afterwards:
 * the shape a real flaky invocation has, so a MAIN failure retries green in RETRY1.
 */
class FlakyRowFixture {

  static final AtomicBoolean FLAKY_ALREADY_FAILED = new AtomicBoolean();

  @ParameterizedTest
  @ValueSource(strings = {"steady1", "flaky", "steady2", "steady3"})
  void rows(String value) {
    if (value.equals("flaky") && FLAKY_ALREADY_FAILED.compareAndSet(false, true)) {
      fail("first execution of the flaky row fails");
    }
    assertThat(value).isNotBlank();
  }
}
