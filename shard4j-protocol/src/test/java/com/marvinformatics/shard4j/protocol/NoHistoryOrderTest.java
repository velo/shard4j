package com.marvinformatics.shard4j.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        "com.example.orders.CartIT$WhenEmpty#total(int[])|b697ce4ef1dbfbdf",
        "com.example.orders.CartIT#slow1()|02eefc9a31204a7a",
        "com.example.orders.CartIT#a()|1f500aaa154684f5",
        "com.example.orders.CartIT#b()|e5e6f776330f44f2",
      })
  void pinsTheOrderKeyToItsKnownAnswer(String key, String firstEightBytesHex) {
    long expected = Long.parseUnsignedLong(firstEightBytesHex, 16);

    assertEquals(expected, new HistoryKey(key).orderKey(), key);
  }

  @Test
  void pinsTheOrderKeyOfTheEmptyKey() {
    assertEquals(Long.parseUnsignedLong("e3b0c44298fc1c14", 16), new HistoryKey("").orderKey());
  }

  @Test
  void sortsAscendingAsAnUnsignedValue() {
    HistoryKey lowest = new HistoryKey("com.example.orders.CartIT#slow1()");
    HistoryKey middle = new HistoryKey("com.example.orders.CartIT#a()");
    HistoryKey highest = new HistoryKey("com.example.orders.CartIT#each(java.lang.String)");

    List<HistoryKey> sorted =
        List.of(highest, lowest, middle).stream().sorted(HistoryKey.NO_HISTORY_ORDER).toList();

    assertEquals(List.of(lowest, middle, highest), sorted);
  }

  @Test
  void treatsEveryTopBitPatternAsAPositiveNumber() {
    HistoryKey topBitClear = new HistoryKey("com.example.orders.CartIT#slow1()");
    HistoryKey topBitSet = new HistoryKey("com.example.orders.CartIT#total()");

    assertTrue(topBitSet.orderKey() < 0, "the fixture must exercise the sign bit");
    assertTrue(
        HistoryKey.NO_HISTORY_ORDER.compare(topBitClear, topBitSet) < 0,
        "0x02ee... must sort before 0xd95e...; a signed compare puts them the other way round");
  }

  @Test
  void breaksAHashTieLexicographically() {
    long tied = 0x00000000000000ffL;

    assertTrue(HistoryKey.compareNoHistory(tied, "a.B#a()", tied, "a.B#b()") < 0);
    assertTrue(HistoryKey.compareNoHistory(tied, "a.B#b()", tied, "a.B#a()") > 0);
    assertEquals(0, HistoryKey.compareNoHistory(tied, "a.B#a()", tied, "a.B#a()"));
  }

  @Test
  void isConsistentWithEquals() {
    HistoryKey key = new HistoryKey("com.example.orders.CartIT#total()");

    assertEquals(0, HistoryKey.NO_HISTORY_ORDER.compare(key, new HistoryKey(key.value())));
  }
}
