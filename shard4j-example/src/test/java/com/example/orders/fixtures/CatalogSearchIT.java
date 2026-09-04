package com.example.orders.fixtures;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Sharding fixture: a parameterized method. It leases as one unit -- invocations do not
 * exist at discovery -- while each row still executes and is recorded individually.
 */
@Tag("shard4j-fixture")
public class CatalogSearchIT {

  @ParameterizedTest
  @ValueSource(strings = {"mug", "shirt", "sticker"})
  void findsProducts(String term) {
    assertThat(term.length()).isGreaterThan(2);
  }
}
