package com.marvinformatics.shard4j.protocol;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;

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

  /**
   * Ascending by {@link #orderKey()} read as an unsigned 64-bit value, ties broken
   * lexicographically by the key itself.
   */
  public static final Comparator<HistoryKey> NO_HISTORY_ORDER =
      (left, right) ->
          compareNoHistory(left.orderKey(), left.value(), right.orderKey(), right.value());

  public static HistoryKey from(ExecutionId id) {
    return new HistoryKey(id.className() + "#" + id.unitSignature());
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
    byte[] digest = sha256(value.getBytes(StandardCharsets.UTF_8));
    long orderKey = 0;
    for (int i = 0; i < Long.BYTES; i++) {
      orderKey = (orderKey << 8) | (digest[i] & 0xffL);
    }
    return orderKey;
  }

  /**
   * The comparison behind {@link #NO_HISTORY_ORDER}, taking the two halves apart so the
   * tie-break is reachable: real SHA-256 keys never collide in 64 bits, and an unreachable
   * branch is an unverifiable one.
   */
  static int compareNoHistory(
      long leftOrderKey, String leftKey, long rightOrderKey, String rightKey) {
    int byOrderKey = Long.compareUnsigned(leftOrderKey, rightOrderKey);
    return byOrderKey != 0 ? byOrderKey : leftKey.compareTo(rightKey);
  }

  private static byte[] sha256(byte[] input) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(input);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required of every JVM", e);
    }
  }
}
