package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.Failure;
import java.io.PrintWriter;
import java.io.StringWriter;
import lombok.experimental.UtilityClass;

/**
 * Renders a test's throwable for the wire. When the trace has to be cut, it keeps the head (where
 * the test failed) and the tail (the root cause), and drops the middle of a deep framework stack,
 * which nobody reads.
 */
@UtilityClass
class FailureCapture {

  final int STACK_TRACE_MAX_CHARS = 32 * 1024;
  final int MESSAGE_MAX_CHARS = 4 * 1024;
  private final int KEPT_TAIL_CHARS = 8 * 1024;

  Failure toTruncatedFailure(Throwable throwable) {
    if (throwable == null) {
      return null;
    }
    StringWriter trace = new StringWriter();
    throwable.printStackTrace(new PrintWriter(trace));
    return new Failure(
        throwable.getClass().getName(),
        truncate(throwable.getMessage(), MESSAGE_MAX_CHARS),
        elideMiddle(trace.toString(), STACK_TRACE_MAX_CHARS));
  }

  String elideMiddle(String text, int maxCharacters) {
    if (text.length() <= maxCharacters) {
      return text;
    }
    int tailLength = Math.min(KEPT_TAIL_CHARS, maxCharacters / 4);
    int headLength = maxCharacters - tailLength - formatOmissionMarker(text.length()).length();
    return text.substring(0, headLength)
        + formatOmissionMarker(text.length() - headLength - tailLength)
        + text.substring(text.length() - tailLength);
  }

  private String formatOmissionMarker(int omittedCharacterCount) {
    return "\n\t... " + omittedCharacterCount + " characters omitted ...\n";
  }

  private String truncate(String text, int maxCharacters) {
    return text == null || text.length() <= maxCharacters ? text : text.substring(0, maxCharacters);
  }
}
