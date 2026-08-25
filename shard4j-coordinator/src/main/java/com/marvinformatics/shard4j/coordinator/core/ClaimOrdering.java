package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.HistoryKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Function;
import lombok.experimental.UtilityClass;

/**
 * The order in which claimable units are granted: slowest first by the measured aggregate,
 * with unknown-duration units ahead of every known one, ordered among themselves by the
 * pinned SHA-256 hash of their history key.
 *
 * <p>"Is this duration known?" is the absence of the key in the store -- no flag, no
 * sentinel estimate. Unknowns are never compared against a known duration, which is what
 * makes the missing-estimate question moot instead of a guessing game.
 *
 * <p>Units arrive already parsed, so ranking the whole claimable pool costs no id parsing
 * at all -- which is what makes the open ask affordable over a whole census rather than
 * one class's candidates.
 */
@UtilityClass
public class ClaimOrdering {

  public List<CensusUnit> order(
      List<CensusUnit> candidates, Function<HistoryKey, OptionalLong> estimates) {
    record Ranked(CensusUnit unit, OptionalLong estimate) {}

    List<Ranked> unknown = new ArrayList<>();
    List<Ranked> known = new ArrayList<>();
    for (CensusUnit candidate : candidates) {
      OptionalLong estimate = estimates.apply(candidate.historyKey());
      (estimate.isPresent() ? known : unknown).add(new Ranked(candidate, estimate));
    }
    unknown.sort(
        Comparator.comparing((Ranked ranked) -> ranked.unit().historyKey(),
            HistoryKey.NO_HISTORY_ORDER));
    known.sort(
        Comparator.comparingLong((Ranked ranked) -> ranked.estimate().getAsLong())
            .reversed()
            .thenComparing(ranked -> ranked.unit().historyKey().value()));

    List<CensusUnit> ordered = new ArrayList<>(candidates.size());
    unknown.forEach(ranked -> ordered.add(ranked.unit()));
    known.forEach(ranked -> ordered.add(ranked.unit()));
    return ordered;
  }
}
