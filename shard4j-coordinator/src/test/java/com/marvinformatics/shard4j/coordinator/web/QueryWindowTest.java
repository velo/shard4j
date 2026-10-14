package com.marvinformatics.shard4j.coordinator.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class QueryWindowTest {

  private static final Instant NOW = Instant.parse("2026-10-14T12:00:00Z");

  @ParameterizedTest
  @CsvSource({
    "30s, PT30S",
    "5m, PT5M",
    "12h, PT12H",
    "2d, PT48H",
    "1w, PT168H",
    "1M, PT720H",
    "PT5M, PT5M",
    "P2D, PT48H"
  })
  void givenShortFormOrIso8601Duration_whenParsing_thenReturnsMatchingDurationWithMonthAsThirtyDays(
      String durationText, Duration expectedDuration) {
    assertThat(QueryWindow.parsePositiveDuration("since", durationText))
        .isEqualTo(expectedDuration);
  }

  @ParameterizedTest
  @ValueSource(strings = {"5", "5x", "-1d", "0d", "PT0S", "1 d", ""})
  void givenAMalformedOrNonPositiveWindow_whenParsing_thenItIsRejectedNamingTheParameter(
      String invalidWindow) {
    assertThatExceptionOfType(InvalidQueryParameterException.class)
        .isThrownBy(() -> QueryWindow.parsePositiveDuration("since", invalidWindow))
        .withMessageStartingWith("since must be");
  }

  @Test
  void
      givenASinceShorterThanRetention_whenBuildingTheWindowEndingNow_thenItSpansTheWholeSinceAndIsNotRetentionLimited() {
    QueryWindow window = QueryWindow.parseSinceClampedToRetention(NOW, "2d", Duration.ofDays(30));

    assertThat(window.from()).isEqualTo(Instant.parse("2026-10-12T12:00:00Z"));
    assertThat(window.to()).isEqualTo(NOW);
    assertThat(window.retentionLimited()).isFalse();
  }

  @Test
  void
      givenSinceLongerThanRetention_whenBuildingTheWindowEndingNow_thenFromIsClampedToRetentionAndRetentionLimitedIsSet() {
    QueryWindow window = QueryWindow.parseSinceClampedToRetention(NOW, "2M", Duration.ofDays(30));

    assertThat(window.from()).isEqualTo(NOW.minus(Duration.ofDays(30)));
    assertThat(window.retentionLimited()).isTrue();
  }
}
