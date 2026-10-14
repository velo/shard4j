package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.Failure;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class FailureCaptureTest {

  @Test
  void
      givenAThrowableWithACause_whenCaptured_thenOuterClassAndMessageAreRecordedAndStackTraceIncludesTheCause() {
    Failure failure =
        FailureCapture.toTruncatedFailure(
            new IllegalStateException("outer", new IllegalArgumentException("root")));

    assertThat(failure.exceptionClass()).isEqualTo(IllegalStateException.class.getName());
    assertThat(failure.message()).isEqualTo("outer");
    assertThat(failure.stackTrace())
        .contains("FailureCaptureTest.givenAThrowableWithACause")
        .contains("Caused by: java.lang.IllegalArgumentException: root");
  }

  @Test
  void
      givenATraceOverTheStackLimit_whenCutToHeadAndTail_thenBothEndsSurviveWithinTheLimitAndTheMarkerCountsEveryOmittedCharacter() {
    String oversizedTrace = "HEAD" + "x".repeat(100_000) + "ROOT-CAUSE";

    String truncatedTrace =
        FailureCapture.elideMiddle(oversizedTrace, FailureCapture.STACK_TRACE_MAX_CHARS);

    assertThat(truncatedTrace).hasSizeLessThanOrEqualTo(FailureCapture.STACK_TRACE_MAX_CHARS);
    assertThat(truncatedTrace).startsWith("HEAD").endsWith("ROOT-CAUSE");
    Matcher omissionMarkerMatch =
        Pattern.compile("\n\t\\.\\.\\. (\\d+) characters omitted \\.\\.\\.\n")
            .matcher(truncatedTrace);
    assertThat(omissionMarkerMatch.find()).isTrue();
    int keptCharacterCount = truncatedTrace.length() - omissionMarkerMatch.group().length();
    assertThat(keptCharacterCount + Integer.parseInt(omissionMarkerMatch.group(1)))
        .isEqualTo(oversizedTrace.length());
  }

  @Test
  void givenAMessageOverTheMessageLimit_whenCaptured_thenTheMessageIsCappedAtExactlyTheLimit() {
    Failure failure = FailureCapture.toTruncatedFailure(new AssertionError("m".repeat(50_000)));

    assertThat(failure.message()).hasSize(FailureCapture.MESSAGE_MAX_CHARS);
  }

  @Test
  void givenNoThrowable_whenCaptured_thenThereIsNoFailure() {
    assertThat(FailureCapture.toTruncatedFailure(null)).isNull();
  }
}
