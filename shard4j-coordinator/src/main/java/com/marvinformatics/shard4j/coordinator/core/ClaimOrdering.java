package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.HistoryKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;
import lombok.experimental.UtilityClass;

/**
 * The order in which claimable units are granted: slowest class first by the measured
 * remaining total of the class, with unknown-duration units ahead of every known one,
 * ordered among themselves by the pinned SHA-256 hash of their history key -- and
 * cardinality probes last of all.
 *
 * <p>The measured tier ranks by <em>class</em> total because a class is what one runner
 * drains: twenty 30s tests outrank a class holding one 120s test, which ranking by the
 * slowest single unit had backwards. Only what is still claimable counts, and inside the
 * chosen class the order stays slowest unit first.
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

  public List<ClaimableUnit> order(
      List<ClaimableUnit> candidates, Function<ClaimableUnit, OptionalLong> estimates) {
    record Ranked(ClaimableUnit unit, OptionalLong estimate) {}

    Comparator<Ranked> hashOrder =
        Comparator.comparing((Ranked ranked) -> ranked.unit().historyKey(), HistoryKey.NO_HISTORY_ORDER)
            .thenComparing(ranked -> ranked.unit().id());

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

    Map<String, Long> classTotals = new HashMap<>();
    for (Ranked ranked : known) {
      classTotals.merge(ranked.unit().className(), ranked.estimate().getAsLong(), Long::sum);
    }

    unknown.sort(hashOrder);
    known.sort(
        Comparator.comparingLong((Ranked ranked) -> classTotals.get(ranked.unit().className()))
            .thenComparingLong(ranked -> ranked.estimate().getAsLong())
            .reversed()
            .thenComparing(ranked -> ranked.unit().historyKey().value())
            .thenComparing(ranked -> ranked.unit().id()));
    probe.sort(hashOrder);

    List<ClaimableUnit> ordered = new ArrayList<>(candidates.size());
    unknown.forEach(ranked -> ordered.add(ranked.unit()));
    known.forEach(ranked -> ordered.add(ranked.unit()));
    probe.forEach(ranked -> ordered.add(ranked.unit()));
    return ordered;
  }
}
