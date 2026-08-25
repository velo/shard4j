package com.marvinformatics.shard4j.protocol;

import static org.assertj.core.api.Assertions.assertThat;

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

    assertThat(older.compareTo(newer) < 0).as(why).isTrue();
    assertThat(newer.compareTo(older) > 0).as(why).isTrue();
  }

  @Test
  void comparesEqualOnlyToItself() {
    Fence fence = new Fence(1, 4, 102);

    assertThat(fence)
        .isEqualByComparingTo(new Fence(1, 4, 102))
        .isEqualTo(new Fence(1, 4, 102));
  }

  @Test
  void survivesCountersThatOverflowASignedInt() {
    Fence beforeTheWrap = new Fence(1, 4, Integer.MAX_VALUE);
    Fence afterTheWrap = new Fence(1, 4, Integer.MAX_VALUE + 1L);

    assertThat(beforeTheWrap).isLessThan(afterTheWrap);
  }

  @Test
  void sortsIntoIssueOrder() {
    Fence first = new Fence(1, 4, 100);
    Fence second = new Fence(1, 4, 101);
    Fence third = new Fence(1, 5, 0);
    Fence fourth = new Fence(2, 5, 0);

    assertThat(List.of(third, fourth, first, second).stream().sorted().toList())
        .containsExactlyElementsOf(List.of(first, second, third, fourth));
  }
}
