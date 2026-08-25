package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.CensusUnit;
import com.marvinformatics.shard4j.protocol.HistoryKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.experimental.UtilityClass;

/**
 * The order in which claimable units are granted: slowest first by the measured aggregate,
 * with unknown-duration units ahead of every known one, ordered among themselves by the
 * pinned SHA-256 hash of their history key -- and cardinality probes last of all.
 *
 * <p>"Is this duration known?" is the absence of the key in the store -- no flag, no
 * sentinel estimate. Unknowns are never compared against a known duration, which is what
 * makes the missing-estimate question moot instead of a guessing game.
 *
 * <p>A probe is not an unknown in the ordering sense: an unknown is a unit that exists and
 * has never been measured, while a probe is a position past a measured parameter count
 * that most runs proves nonexistent. Ranking probes with the unknowns would drag every
 * distributed template's class into the unknowns tier on every run and dissolve cross-class
 * slowest-first; ranking them last costs nothing when the probe vanishes and one run of
 * measurement lag when it is real.
 *
 * <p>Units arrive already parsed, so ranking the whole claimable pool costs no id parsing
 * at all -- which is what makes the open ask affordable over a whole census rather than
 * one class's candidates.
 */
@UtilityClass
public class ClaimOrdering {

  public List<CensusUnit> order(
      List<CensusUnit> candidates,
      Function<CensusUnit, OptionalLong> estimates,
      Predicate<CensusUnit> probes) {
    record Ranked(CensusUnit unit, OptionalLong estimate) {}

    Comparator<Ranked> hashOrder =
        Comparator.comparing((Ranked ranked) -> ranked.unit().historyKey(), HistoryKey.NO_HISTORY_ORDER)
            .thenComparing(ranked -> ranked.unit().id());

    List<Ranked> unknown = new ArrayList<>();
    List<Ranked> known = new ArrayList<>();
    List<Ranked> probe = new ArrayList<>();
    for (CensusUnit candidate : candidates) {
      if (probes.test(candidate)) {
        probe.add(new Ranked(candidate, OptionalLong.empty()));
        continue;
      }
      OptionalLong estimate = estimates.apply(candidate);
      (estimate.isPresent() ? known : unknown).add(new Ranked(candidate, estimate));
    }
    unknown.sort(hashOrder);
    known.sort(
        Comparator.comparingLong((Ranked ranked) -> ranked.estimate().getAsLong())
            .reversed()
            .thenComparing(ranked -> ranked.unit().historyKey().value())
            .thenComparing(ranked -> ranked.unit().id()));
    probe.sort(hashOrder);

    List<CensusUnit> ordered = new ArrayList<>(candidates.size());
    unknown.forEach(ranked -> ordered.add(ranked.unit()));
    known.forEach(ranked -> ordered.add(ranked.unit()));
    probe.forEach(ranked -> ordered.add(ranked.unit()));
    return ordered;
  }
}
