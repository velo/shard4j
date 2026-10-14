package com.marvinformatics.shard4j.coordinator.stats;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;

/**
 * Groups failures that are the same fault. The message is left out because it routinely
 * carries a timestamp, a port or a generated id; line numbers are left out because they
 * move with every unrelated edit to the file. What stays is the exception type and the
 * frames that led to it, cause chain included -- the root cause is often the only line
 * that tells two otherwise identical timeouts apart.
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
  private final Pattern LAMBDA = Pattern.compile("\\$\\$Lambda[^.(]*");
  private final Pattern HIDDEN_CLASS_ADDRESS = Pattern.compile("/0x[0-9a-f]+");
  private final Pattern CAUSE = Pattern.compile("^(Caused by|Suppressed): ([^:\\s]+).*");

  public String of(String exceptionClass, String stackTrace) {
    StringBuilder canonical = new StringBuilder(exceptionClass).append('\n');
    int frames = 0;
    for (String raw : stackTrace.split("\n")) {
      String line = raw.strip();
      Matcher cause = CAUSE.matcher(line);
      if (cause.matches()) {
        canonical.append(cause.group(1)).append(' ').append(cause.group(2)).append('\n');
        frames = 0;
      } else if (line.startsWith("at ") && frames++ < FRAMES_PER_THROWABLE) {
        String frame = SOURCE_LOCATION.matcher(line).replaceAll("");
        frame = LAMBDA.matcher(frame).replaceAll(Matcher.quoteReplacement("$$Lambda"));
        canonical.append(HIDDEN_CLASS_ADDRESS.matcher(frame).replaceAll("")).append('\n');
      }
    }
    return sha256(canonical.toString());
  }

  private String sha256(String text) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest, 0, 16);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required of every JVM", e);
    }
  }
}
