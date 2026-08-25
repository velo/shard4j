package com.marvinformatics.shard4j.coordinator.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.HistoryKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ClaimOrderingTest {

  private static String id(String method) {
    return "[engine:junit-jupiter]/[class:com.example.orders.OrderIT]/[method:" + method + "()]";
  }

  @Test
  void knownDurationsComeSlowestFirst() {
    Map<String, Long> estimates =
        Map.of(
            "com.example.orders.OrderIT#fast()", 1_300L,
            "com.example.orders.OrderIT#slow()", 702_000L,
            "com.example.orders.OrderIT#mid()", 90_000L);
    List<String> ordered =
        ClaimOrdering.order(
            List.of(id("fast"), id("mid"), id("slow")),
            key ->
                estimates.containsKey(key.value())
                    ? OptionalLong.of(estimates.get(key.value()))
                    : OptionalLong.empty());
    assertThat(ordered).containsExactly(id("slow"), id("mid"), id("fast"));
  }

  @Test
  void noHistoryMeansPinnedHashOrder() {
    List<String> ids = List.of(id("aaa"), id("bbb"), id("ccc"), id("ddd"));
    List<String> ordered = ClaimOrdering.order(ids, key -> OptionalLong.empty());

    List<String> expected = new ArrayList<>(ids);
    expected.sort(
        (left, right) ->
            HistoryKey.NO_HISTORY_ORDER.compare(HistoryKeys.of(left), HistoryKeys.of(right)));
    assertThat(ordered).isEqualTo(expected);
  }

  @Test
  void unknownsAlwaysPrecedeKnownsAndAreNeverComparedToThem() {
    List<String> ordered =
        ClaimOrdering.order(
            List.of(id("known"), id("mystery"), id("alsoKnown")),
            key ->
                key.value().contains("mystery") ? OptionalLong.empty() : OptionalLong.of(50_000L));
    assertThat(ordered.get(0)).isEqualTo(id("mystery"));
    assertThat(ordered).hasSize(3);
  }

  @Test
  void equalKnownDurationsTieBreakOnTheKeyItself() {
    List<String> ordered =
        ClaimOrdering.order(
            List.of(id("zed"), id("alpha")), key -> OptionalLong.of(10_000L));
    assertThat(ordered).containsExactly(id("alpha"), id("zed"));
  }
}
