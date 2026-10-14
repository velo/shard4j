package com.marvinformatics.shard4j.coordinator.web;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The time window a statistics query covers, ending now. A window reaching past retention
 * is cut to it and says so, rather than silently answering for less than was asked.
 *
 * <p>Durations are a count and a unit, case-sensitive because {@code m} and {@code M} are
 * both wanted: {@code 30s}, {@code 5m}, {@code 12h}, {@code 2d}, {@code 1w}, {@code 1M}
 * (30 days). ISO-8601 ({@code PT5M}, {@code P2D}) is accepted too.
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

  public static QueryWindow ending(Instant now, String since, Duration retention) {
    Duration asked = parse("since", since);
    boolean limited = asked.compareTo(retention) > 0;
    return new QueryWindow(now.minus(limited ? retention : asked), now, limited);
  }

  public static Duration parse(String parameter, String value) {
    Matcher shortForm = SHORT_FORM.matcher(value);
    Duration parsed;
    if (shortForm.matches()) {
      parsed = UNITS.get(shortForm.group(2)).multipliedBy(Long.parseLong(shortForm.group(1)));
    } else {
      try {
        parsed = Duration.parse(value);
      } catch (DateTimeParseException e) {
        throw new InvalidQueryException(
            parameter
                + " must be a count and a unit (s, m, h, d, w, M -- m is minutes, M is 30 days)"
                + " or ISO-8601: "
                + value);
      }
    }
    if (parsed.isNegative() || parsed.isZero()) {
      throw new InvalidQueryException(parameter + " must be positive: " + value);
    }
    return parsed;
  }
}
