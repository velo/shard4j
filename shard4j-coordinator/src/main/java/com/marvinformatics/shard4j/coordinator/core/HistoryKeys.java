package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.HistoryKey;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;

/**
 * The single place a wire execution id is taken apart.
 *
 * <p>The engine derives the same history key from a live {@code MethodSource}; the
 * coordinator only ever sees the wire form, so here the derivation is the string prefix
 * rule: drop the engine segment, drop a trailing invocation segment, join class and method
 * with {@code #}. Shape A (plain method), shape B (template method) and shape C (template
 * invocation) all collapse to one key per lease unit, which is exactly what the scheduler
 * hands out.
 *
 * <p>Callers that need the class name take it from the {@link CensusUnit} this returns
 * rather than cutting the id up themselves: one grammar, one error message, one parse per
 * unit per session.
 */
@UtilityClass
public class HistoryKeys {

  private final Pattern EXECUTION_ID =
      Pattern.compile(
          "\\[engine:junit-jupiter\\]"
              + "/\\[class:(?<className>[^\\]]+)\\]"
              + "/\\[(?:method|test-template):(?<method>[^\\]]+)\\]"
              + "(?:/\\[test-template-invocation:#\\d+\\])?");

  public CensusUnit parse(String executionId) {
    Matcher matcher = EXECUTION_ID.matcher(executionId);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "Not a claimable execution id shape (class plus method or test-template, rooted at"
              + " junit-jupiter): "
              + executionId);
    }
    String className = matcher.group("className");
    return new CensusUnit(
        executionId, className, new HistoryKey(className + "#" + matcher.group("method")));
  }

  public HistoryKey of(String executionId) {
    return parse(executionId).historyKey();
  }
}
