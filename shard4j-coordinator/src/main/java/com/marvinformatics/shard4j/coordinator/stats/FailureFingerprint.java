package com.marvinformatics.shard4j.coordinator.stats;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;

/**
 * Groups failures that are the same fault. The message is left out because it usually holds a
 * timestamp, a port or a generated id. Line numbers are left out because they change with every
 * unrelated edit to the file. What stays is the exception type and the frames that led to it, cause
 * chain included: the root cause is often the only line that tells two otherwise identical timeouts
 * apart.
 *
 * <p>For example, these two traces share a fingerprint:
 *
 * <pre>
 * java.net.SocketTimeoutException: Read timed out after 30001 ms
 *     at com.example.orders.GatewayClient.call(GatewayClient.java:88)
 *     at com.example.orders.CheckoutIT$$Lambda/0x00007f3c4c0a1234.run(Unknown Source)
 *
 * java.net.SocketTimeoutException: Read timed out after 30017 ms
 *     at com.example.orders.GatewayClient.call(GatewayClient.java:91)
 *     at com.example.orders.CheckoutIT$$Lambda/0x00007f3c4c0b9876.run(Unknown Source)
 * </pre>
 */
@UtilityClass
public class FailureFingerprint {

  private final int FRAMES_PER_THROWABLE = 20;
  private final Pattern SOURCE_LOCATION = Pattern.compile("\\([^)]*\\)$");
  private final Pattern LAMBDA_CLASS_SUFFIX = Pattern.compile("\\$\\$Lambda[^.(]*");
  private final Pattern HIDDEN_CLASS_ADDRESS = Pattern.compile("/0x[0-9a-f]+");
  private final Pattern NESTED_THROWABLE_HEADER =
      Pattern.compile("^(Caused by|Suppressed): ([^:\\s]+).*");

  public String hashNormalizedTrace(String exceptionClass, String stackTrace) {
    StringBuilder canonicalTrace = new StringBuilder(exceptionClass).append('\n');
    int framesSeenInThrowable = 0;
    for (String rawLine : stackTrace.split("\n")) {
      String line = rawLine.strip();
      Matcher causeHeaderMatch = NESTED_THROWABLE_HEADER.matcher(line);
      if (causeHeaderMatch.matches()) {
        canonicalTrace
            .append(causeHeaderMatch.group(1))
            .append(' ')
            .append(causeHeaderMatch.group(2))
            .append('\n');
        framesSeenInThrowable = 0;
      } else if (line.startsWith("at ") && framesSeenInThrowable++ < FRAMES_PER_THROWABLE) {
        String frame = SOURCE_LOCATION.matcher(line).replaceAll("");
        frame = LAMBDA_CLASS_SUFFIX.matcher(frame).replaceAll(Matcher.quoteReplacement("$$Lambda"));
        canonicalTrace.append(HIDDEN_CLASS_ADDRESS.matcher(frame).replaceAll("")).append('\n');
      }
    }
    return hashToSha256HexPrefix(canonicalTrace.toString());
  }

  private String hashToSha256HexPrefix(String text) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest, 0, 16);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required of every JVM", e);
    }
  }
}
