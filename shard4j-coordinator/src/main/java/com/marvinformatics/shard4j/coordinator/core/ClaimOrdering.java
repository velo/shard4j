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
 */
@UtilityClass
public class ClaimOrdering {

  public List<String> order(
      List<String> candidates, Function<HistoryKey, OptionalLong> estimates) {
    record Ranked(String id, HistoryKey key, OptionalLong estimate) {}

    List<Ranked> unknown = new ArrayList<>();
    List<Ranked> known = new ArrayList<>();
    for (String id : candidates) {
      HistoryKey key = HistoryKeys.of(id);
      OptionalLong estimate = estimates.apply(key);
      (estimate.isPresent() ? known : unknown).add(new Ranked(id, key, estimate));
    }
    unknown.sort(Comparator.comparing(Ranked::key, HistoryKey.NO_HISTORY_ORDER));
    known.sort(
        Comparator.comparingLong((Ranked ranked) -> ranked.estimate().getAsLong())
            .reversed()
            .thenComparing(ranked -> ranked.key().value()));

    List<String> ordered = new ArrayList<>(candidates.size());
    unknown.forEach(ranked -> ordered.add(ranked.id()));
    known.forEach(ranked -> ordered.add(ranked.id()));
    return ordered;
  }
}
