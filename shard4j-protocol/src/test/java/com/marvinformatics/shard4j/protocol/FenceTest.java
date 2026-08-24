package com.marvinformatics.shard4j.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FenceTest {

  @ParameterizedTest(name = "[{index}] {0}")
  @CsvSource(
      delimiter = '|',
      value = {
        "a bumped epoch outranks everything|1|9|9|2|0|0",
        "a newer incarnation outranks a higher seq|1|4|900|1|5|0",
        "seq is the last word|1|4|101|1|4|102",
      })
  void ordersLexicographically(
      String why, long epoch, long incarnation, long seq, long higherEpoch,
      long higherIncarnation, long higherSeq) {
    Fence older = new Fence(epoch, incarnation, seq);
    Fence newer = new Fence(higherEpoch, higherIncarnation, higherSeq);

    assertTrue(older.compareTo(newer) < 0, why);
    assertTrue(newer.compareTo(older) > 0, why);
  }

  @Test
  void comparesEqualOnlyToItself() {
    Fence fence = new Fence(1, 4, 102);

    assertEquals(0, fence.compareTo(new Fence(1, 4, 102)));
    assertEquals(new Fence(1, 4, 102), fence);
  }

  @Test
  void survivesCountersThatOverflowASignedInt() {
    Fence beforeTheWrap = new Fence(1, 4, Integer.MAX_VALUE);
    Fence afterTheWrap = new Fence(1, 4, Integer.MAX_VALUE + 1L);

    assertTrue(beforeTheWrap.compareTo(afterTheWrap) < 0);
  }

  @Test
  void sortsIntoIssueOrder() {
    Fence first = new Fence(1, 4, 100);
    Fence second = new Fence(1, 4, 101);
    Fence third = new Fence(1, 5, 0);
    Fence fourth = new Fence(2, 5, 0);

    assertEquals(
        List.of(first, second, third, fourth),
        List.of(third, fourth, first, second).stream().sorted().toList());
  }
}
