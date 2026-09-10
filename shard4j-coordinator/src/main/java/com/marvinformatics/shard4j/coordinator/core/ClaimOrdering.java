package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.HistoryKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;
import lombok.experimental.UtilityClass;

/**
 * The order in which claimable units are granted: unknown-duration units first, in the
 * pinned SHA-256 hash order of their history key, then measured classes by their remaining
 * total and slowest unit first within each, and cardinality probes last of all.
 *
 * <p>The measured tier ranks whole classes because a class is what one runner drains:
 * twenty 30s tests outrank a class holding one 120s test, which ranking by the slowest
 * single unit had backwards. Only what is still claimable counts, so a half-drained class
 * sinks as it empties. A class's units stay contiguous in the result, which is the property
 * the scheduler reads it for.
 *
 * <p>"Is this duration known?" is the absence of the key in the store -- no flag, no
 * sentinel estimate. Unknowns are never compared against a known duration, which makes the
 * missing-estimate question moot: a class holding one reaches the front through that unit
 * rather than through a total.
 *
 * <p>A probe is not an unknown in the ordering sense: an unknown is a unit that exists and
 * has never been measured, while a probe is a position past a measured parameter count
 * that most runs proves nonexistent. Ranking probes with the unknowns would drag every
 * distributed template's class into the unknowns tier on every run; ranking them last costs
 * one run of measurement lag when the probe is real. They are left out of the class total
 * for the same reason.
 *
 * <p>Units arrive already parsed and already carrying their probe flag, so ranking the
 * whole claimable pool costs no id parsing and no session lookups -- which is what makes
 * the open ask affordable over a whole census rather than one class's candidates.
 */
@UtilityClass
public class ClaimOrdering {

  private record Ranked(ClaimableUnit unit, OptionalLong estimate) {}

  private record ClassGroup(String className, long total, List<Ranked> units) {}

  private static final Comparator<Ranked> HASH_ORDER =
      Comparator.comparing((Ranked ranked) -> ranked.unit().historyKey(), HistoryKey.NO_HISTORY_ORDER)
          .thenComparing(ranked -> ranked.unit().id());

  private static final Comparator<Ranked> SLOWEST_FIRST =
      Comparator.comparingLong((Ranked ranked) -> ranked.estimate().getAsLong())
          .reversed()
          .thenComparing(ranked -> ranked.unit().historyKey().value())
          .thenComparing(ranked -> ranked.unit().id());

  private static final Comparator<ClassGroup> HEAVIEST_FIRST =
      Comparator.comparingLong(ClassGroup::total).reversed().thenComparing(ClassGroup::className);

  public List<ClaimableUnit> order(
      List<ClaimableUnit> candidates, Function<ClaimableUnit, OptionalLong> estimates) {
    List<Ranked> unknown = new ArrayList<>();
    List<Ranked> known = new ArrayList<>();
    List<Ranked> probe = new ArrayList<>();
    for (ClaimableUnit candidate : candidates) {
      if (candidate.probe()) {
        probe.add(new Ranked(candidate, OptionalLong.empty()));
        continue;
      }
      OptionalLong estimate = estimates.apply(candidate);
      (estimate.isPresent() ? known : unknown).add(new Ranked(candidate, estimate));
    }
    unknown.sort(HASH_ORDER);
    probe.sort(HASH_ORDER);

    List<ClaimableUnit> ordered = new ArrayList<>(candidates.size());
    unknown.forEach(ranked -> ordered.add(ranked.unit()));
    for (ClassGroup group : groupByClass(known)) {
      group.units().forEach(ranked -> ordered.add(ranked.unit()));
    }
    probe.forEach(ranked -> ordered.add(ranked.unit()));
    return ordered;
  }

  private List<ClassGroup> groupByClass(List<Ranked> known) {
    Map<String, List<Ranked>> byClass = new LinkedHashMap<>();
    for (Ranked ranked : known) {
      byClass.computeIfAbsent(ranked.unit().className(), name -> new ArrayList<>()).add(ranked);
    }
    List<ClassGroup> groups = new ArrayList<>(byClass.size());
    byClass.forEach(
        (className, units) -> {
          units.sort(SLOWEST_FIRST);
          long total = units.stream().mapToLong(ranked -> ranked.estimate().getAsLong()).sum();
          groups.add(new ClassGroup(className, total, units));
        });
    groups.sort(HEAVIEST_FIRST);
    return groups;
  }
}
