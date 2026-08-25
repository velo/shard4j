package com.marvinformatics.shard4j.coordinator.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.HistoryKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class ClaimOrderingTest {

  private static String id(String method) {
    return "[engine:junit-jupiter]/[class:com.example.orders.OrderIT]/[method:" + method + "()]";
  }

  private static CensusUnit unit(String method) {
    return HistoryKeys.parse(id(method));
  }

  private static List<String> orderedIds(
      List<CensusUnit> candidates, Function<HistoryKey, OptionalLong> estimates) {
    return ClaimOrdering.order(candidates, unit -> estimates.apply(unit.historyKey()), unit -> false)
        .stream()
        .map(CensusUnit::id)
        .toList();
  }

  @Test
  void knownDurationsComeSlowestFirst() {
    Map<String, Long> estimates =
        Map.of(
            "com.example.orders.OrderIT#fast()", 1_300L,
            "com.example.orders.OrderIT#slow()", 702_000L,
            "com.example.orders.OrderIT#mid()", 90_000L);
    List<String> ordered =
        orderedIds(
            List.of(unit("fast"), unit("mid"), unit("slow")),
            key ->
                estimates.containsKey(key.value())
                    ? OptionalLong.of(estimates.get(key.value()))
                    : OptionalLong.empty());
    assertThat(ordered).containsExactly(id("slow"), id("mid"), id("fast"));
  }

  @Test
  void noHistoryMeansPinnedHashOrder() {
    List<String> ids = List.of(id("aaa"), id("bbb"), id("ccc"), id("ddd"));
    List<String> ordered =
        orderedIds(ids.stream().map(HistoryKeys::parse).toList(), key -> OptionalLong.empty());

    List<String> expected = new ArrayList<>(ids);
    expected.sort(
        (left, right) ->
            HistoryKey.NO_HISTORY_ORDER.compare(HistoryKeys.of(left), HistoryKeys.of(right)));
    assertThat(ordered).containsExactlyElementsOf(expected);
  }

  @Test
  void unknownsAlwaysPrecedeKnownsAndAreNeverComparedToThem() {
    List<String> ordered =
        orderedIds(
            List.of(unit("known"), unit("mystery"), unit("alsoKnown")),
            key ->
                key.value().contains("mystery") ? OptionalLong.empty() : OptionalLong.of(50_000L));
    assertThat(ordered.get(0)).isEqualTo(id("mystery"));
    assertThat(ordered).hasSize(3);
  }

  @Test
  void probesComeLastEvenBehindKnownsAndOtherUnknowns() {
    String template =
        "[engine:junit-jupiter]/[class:com.example.orders.OrderIT]"
            + "/[test-template:rows(java.lang.String)]";
    CensusUnit measured = HistoryKeys.parse(template + "/[test-template-invocation:#1]");
    CensusUnit probe = HistoryKeys.parse(template + "/[test-template-invocation:#2]");
    List<CensusUnit> ordered =
        ClaimOrdering.order(
            List.of(probe, measured, unit("mystery"), unit("known")),
            unit ->
                unit.historyKey().value().contains("mystery")
                    ? OptionalLong.empty()
                    : OptionalLong.of(10_000L),
            unit -> unit == probe);
    assertThat(ordered).containsExactly(unit("mystery"), unit("known"), measured, probe);
  }

  @Test
  void equalKnownDurationsTieBreakOnTheKeyItself() {
    List<String> ordered =
        orderedIds(List.of(unit("zed"), unit("alpha")), key -> OptionalLong.of(10_000L));
    assertThat(ordered).containsExactly(id("alpha"), id("zed"));
  }
}
