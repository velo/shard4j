package com.marvinformatics.shard4j.protocol;

import static com.marvinformatics.shard4j.protocol.HistoryKey.compareNoHistory;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The no-history order decides execution order for every test the store has never seen, so
 * the hash is pinned to fixed known answers. The expected values are the first 8 bytes of
 * {@code sha256sum} over the exact key bytes, produced outside this codebase.
 */
class NoHistoryOrderTest {

  @ParameterizedTest(name = "[{index}] {0}")
  @CsvSource(
      delimiter = '|',
      value = {
        "com.example.orders.CartIT#total()|d95effa4ece2c537",
        "com.example.orders.CartIT#each(java.lang.String)|f17c61f68ca246ce",
        "com.example.orders.PingResourceIT#hello()|95f07e048e4edd89",
        "com.example.orders.CartIT$WhenEmpty#total([I)|26ad58c81459d4ee",
        "com.example.orders.CartIT#slow1()|02eefc9a31204a7a",
        "com.example.orders.CartIT#a()|1f500aaa154684f5",
        "com.example.orders.CartIT#b()|e5e6f776330f44f2",
      })
  void pinsTheOrderKeyToItsKnownAnswer(String key, String firstEightBytesHex) {
    long expected = Long.parseUnsignedLong(firstEightBytesHex, 16);

    assertThat(new HistoryKey(key).orderKey()).as(key).isEqualTo(expected);
  }

  @Test
  void pinsTheOrderKeyOfTheEmptyKey() {
    assertThat(new HistoryKey("").orderKey())
        .isEqualTo(Long.parseUnsignedLong("e3b0c44298fc1c14", 16));
  }

  @Test
  void sortsAscendingAsAnUnsignedValue() {
    HistoryKey lowest = new HistoryKey("com.example.orders.CartIT#slow1()");
    HistoryKey middle = new HistoryKey("com.example.orders.CartIT#a()");
    HistoryKey highest = new HistoryKey("com.example.orders.CartIT#each(java.lang.String)");

    List<HistoryKey> sorted =
        List.of(highest, lowest, middle).stream().sorted(HistoryKey.NO_HISTORY_ORDER).toList();

    assertThat(sorted).isEqualTo(List.of(lowest, middle, highest));
  }

  @Test
  void treatsEveryTopBitPatternAsAPositiveNumber() {
    HistoryKey topBitClear = new HistoryKey("com.example.orders.CartIT#slow1()");
    HistoryKey topBitSet = new HistoryKey("com.example.orders.CartIT#total()");

    assertThat(topBitSet.orderKey() < 0).as("the fixture must exercise the sign bit").isTrue();
    assertThat(HistoryKey.NO_HISTORY_ORDER.compare(topBitClear, topBitSet) < 0)
        .as("0x02ee... must sort before 0xd95e...; a signed compare puts them the other way round")
        .isTrue();
  }

  @Test
  void breaksAHashTieLexicographically() {
    long tied = 0x00000000000000ffL;

    assertThat(compareNoHistory(tied, "a.B#a()", tied, "a.B#b()")).isLessThan(0);
    assertThat(compareNoHistory(tied, "a.B#b()", tied, "a.B#a()")).isGreaterThan(0);
    assertThat(compareNoHistory(tied, "a.B#a()", tied, "a.B#a()")).isZero();
  }

  @Test
  void isConsistentWithEquals() {
    HistoryKey key = new HistoryKey("com.example.orders.CartIT#total()");

    assertThat(HistoryKey.NO_HISTORY_ORDER.compare(key, new HistoryKey(key.value()))).isZero();
  }
}
