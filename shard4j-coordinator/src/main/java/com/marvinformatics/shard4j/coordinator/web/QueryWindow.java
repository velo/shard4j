package com.marvinformatics.shard4j.coordinator.web;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The time window a statistics query covers. A window that reaches past retention is cut to it and
 * says so.
 *
 * <p>Durations are a count and a unit, case-sensitive because {@code m} and {@code M} are both
 * wanted: {@code 30s}, {@code 5m}, {@code 12h}, {@code 2d}, {@code 1w}, {@code 1M} (30 days).
 * ISO-8601 ({@code PT5M}, {@code P2D}) is accepted too.
 */
public record QueryWindow(Instant from, Instant to, boolean retentionLimited) {

  private static final Pattern SHORT_FORM = Pattern.compile("(\\d{1,9})(s|m|h|d|w|M)");
  private static final Map<String, Duration> UNITS =
      Map.of(
          "s", Duration.ofSeconds(1),
          "m", Duration.ofMinutes(1),
          "h", Duration.ofHours(1),
          "d", Duration.ofDays(1),
          "w", Duration.ofDays(7),
          "M", Duration.ofDays(30));

  public static QueryWindow parseSinceClampedToRetention(
      Instant now, String since, Duration retention) {
    Duration requestedSpan = parsePositiveDuration("since", since);
    boolean retentionLimited = requestedSpan.compareTo(retention) > 0;
    return new QueryWindow(
        now.minus(retentionLimited ? retention : requestedSpan), now, retentionLimited);
  }

  public static QueryWindow before(Instant end, Duration span, Instant retainedFrom) {
    Instant from = end.minus(span);
    return new QueryWindow(from, end, from.isBefore(retainedFrom));
  }

  public static Duration parsePositiveDuration(String parameterName, String durationText) {
    Matcher shortForm = SHORT_FORM.matcher(durationText);
    Duration span;
    if (shortForm.matches()) {
      span = UNITS.get(shortForm.group(2)).multipliedBy(Long.parseLong(shortForm.group(1)));
    } else {
      try {
        span = Duration.parse(durationText);
      } catch (DateTimeParseException e) {
        throw new InvalidQueryParameterException(
            parameterName
                + " must be a count and a unit (s, m, h, d, w, M -- m is minutes, M is 30 days)"
                + " or ISO-8601: "
                + durationText);
      }
    }
    if (span.isNegative() || span.isZero()) {
      throw new InvalidQueryParameterException(
          parameterName + " must be positive: " + durationText);
    }
    return span;
  }
}
