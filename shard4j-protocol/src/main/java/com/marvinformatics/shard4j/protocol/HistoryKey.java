package com.marvinformatics.shard4j.protocol;

/**
 * The duration-history key: {@code <FQCN>#<methodName>(<paramTypes>)}.
 *
 * <p>A pure string-derived prefix of any execution-id shape -- drop the engine segments,
 * drop a trailing invocation segment, join class and method with {@code #}. One key per
 * lease unit exactly, so every invocation record of a template rolls up to its method's
 * key. Parameter types stay (they disambiguate overloads); parameter values and the
 * invocation index do not.
 */
public record HistoryKey(String value) {

  public static HistoryKey from(ExecutionId id) {
    throw new UnsupportedOperationException("not implemented");
  }

  /**
   * Ordering key for a test with no duration history: the first 8 bytes, big-endian
   * unsigned, of {@code SHA-256(UTF-8(historyKey))}, ascending, ties broken by
   * lexicographic comparison of the key itself.
   *
   * <p>Pinned to SHA-256 on purpose. {@code String.hashCode()} plus {@code Math.abs} is
   * what silently skipped one test in 2^32 in the scheme this replaces.
   */
  public long orderKey() {
    throw new UnsupportedOperationException("not implemented");
  }
}
