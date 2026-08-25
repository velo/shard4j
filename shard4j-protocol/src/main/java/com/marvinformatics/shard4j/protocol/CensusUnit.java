package com.marvinformatics.shard4j.protocol;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The parsed form of a claimable execution id: the wire id plus the facts derived from it
 * exactly once -- class name, method-level history key, template flag and, for a template
 * invocation, its position.
 *
 * <p>This is the single place the execution-id grammar is taken apart or put together.
 * Shape A (plain method), shape B (template method) and shape C (template invocation) all
 * collapse to one {@link HistoryKey} per method -- which is what keeps duration storage at
 * method level even when the scheduler hands out shape-C units. Callers that need the
 * class name, the template flag or the position read these fields instead of cutting the
 * id up themselves: one grammar, one error message, one parse per unit.
 *
 * <p>A position is an {@code int} everywhere; its {@code #N} rendering exists only inside
 * the id itself, written by {@link #atPosition(int)} and read by {@link #parse(String)}.
 * {@code invocation} is null for a whole method or whole template.
 */
public record CensusUnit(
    String id, String className, HistoryKey historyKey, boolean template, Integer invocation) {

  private static final Pattern EXECUTION_ID =
      Pattern.compile(
          "\\[engine:junit-jupiter\\]"
              + "/\\[class:(?<className>[^\\]]+)\\]"
              + "/\\[(?<kind>method|test-template):(?<method>[^\\]]+)\\]"
              + "(?:/\\[test-template-invocation:#(?<invocation>\\d+)\\])?");

  public static CensusUnit parse(String executionId) {
    Matcher matcher = EXECUTION_ID.matcher(executionId);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "Not a claimable execution id shape (class plus method or test-template, rooted at"
              + " junit-jupiter): "
              + executionId);
    }
    String className = matcher.group("className");
    String invocation = matcher.group("invocation");
    return new CensusUnit(
        executionId,
        className,
        new HistoryKey(className + "#" + matcher.group("method")),
        "test-template".equals(matcher.group("kind")),
        invocation == null ? null : Integer.valueOf(invocation));
  }

  public static HistoryKey historyKeyOf(String executionId) {
    return parse(executionId).historyKey();
  }

  /**
   * The composition boundary: the one place a position becomes an id segment. Built
   * directly from the parsed template -- never composed as a string and parsed back.
   */
  public CensusUnit atPosition(int position) {
    if (!template || invocation != null) {
      throw new IllegalArgumentException(
          "Only a whole test-template can be addressed by position: " + id);
    }
    return new CensusUnit(
        id + "/[test-template-invocation:#" + position + "]",
        className,
        historyKey,
        true,
        position);
  }
}
