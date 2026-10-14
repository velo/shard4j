package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.Failure;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class FailureCaptureTest {

  @Test
  void givenAThrowableWithACause_whenCaptured_thenTheWholeChainTravels() {
    Failure failure =
        FailureCapture.of(new IllegalStateException("outer", new IllegalArgumentException("root")));

    assertThat(failure.exceptionClass()).isEqualTo(IllegalStateException.class.getName());
    assertThat(failure.message()).isEqualTo("outer");
    assertThat(failure.stackTrace())
        .contains("FailureCaptureTest.givenAThrowableWithACause")
        .contains("Caused by: java.lang.IllegalArgumentException: root");
  }

  @Test
  void givenATraceOverTheLimit_whenCut_thenHeadAndTailSurviveWithinTheLimit() {
    String text = "HEAD" + "x".repeat(100_000) + "ROOT-CAUSE";

    String cut = FailureCapture.headAndTail(text, FailureCapture.STACK_LIMIT);

    assertThat(cut).hasSizeLessThanOrEqualTo(FailureCapture.STACK_LIMIT);
    assertThat(cut).startsWith("HEAD").endsWith("ROOT-CAUSE");
    Matcher marker = Pattern.compile("\n\t\\.\\.\\. (\\d+) characters omitted \\.\\.\\.\n").matcher(cut);
    assertThat(marker.find()).isTrue();
    int kept = cut.length() - marker.group().length();
    assertThat(kept + Integer.parseInt(marker.group(1))).isEqualTo(text.length());
  }

  @Test
  void givenAHugeMessage_whenCaptured_thenItIsCapped() {
    Failure failure = FailureCapture.of(new AssertionError("m".repeat(50_000)));

    assertThat(failure.message()).hasSize(FailureCapture.MESSAGE_LIMIT);
  }

  @Test
  void givenNoThrowable_whenCaptured_thenThereIsNoFailure() {
    assertThat(FailureCapture.of(null)).isNull();
  }
}
