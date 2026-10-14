package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.Failure;
import java.io.PrintWriter;
import java.io.StringWriter;
import lombok.experimental.UtilityClass;

/**
 * Renders a test's throwable for the wire. The trace keeps its head and its tail when it
 * has to be cut: the head is where the test failed, the tail is the root cause, and the
 * middle of a deep framework stack is what nobody reads.
 */
@UtilityClass
class FailureCapture {

  final int STACK_LIMIT = 32 * 1024;
  final int MESSAGE_LIMIT = 4 * 1024;
  private final int TAIL = 8 * 1024;

  Failure of(Throwable throwable) {
    if (throwable == null) {
      return null;
    }
    StringWriter trace = new StringWriter();
    throwable.printStackTrace(new PrintWriter(trace));
    return new Failure(
        throwable.getClass().getName(),
        capped(throwable.getMessage(), MESSAGE_LIMIT),
        headAndTail(trace.toString(), STACK_LIMIT));
  }

  String headAndTail(String text, int limit) {
    if (text.length() <= limit) {
      return text;
    }
    int tail = Math.min(TAIL, limit / 4);
    int head = limit - tail - omitted(text.length()).length();
    return text.substring(0, head)
        + omitted(text.length() - head - tail)
        + text.substring(text.length() - tail);
  }

  private String omitted(int characters) {
    return "\n\t... " + characters + " characters omitted ...\n";
  }

  private String capped(String text, int limit) {
    return text == null || text.length() <= limit ? text : text.substring(0, limit);
  }
}
