package com.marvinformatics.shard4j.protocol;

import java.util.ArrayList;
import java.util.List;

/** One {@code [type:value]} element of a JUnit unique id. */
record Segment(String type, String value) {

  static List<Segment> split(String raw) {
    List<Segment> segments = new ArrayList<>();
    for (String part : raw.split("/", -1)) {
      segments.add(parse(part, raw));
    }
    return segments;
  }

  private static Segment parse(String part, String raw) {
    if (part.length() < 2 || part.charAt(0) != '[' || part.charAt(part.length() - 1) != ']') {
      throw new MalformedExecutionIdException(raw, "segment is not [type:value]: " + part);
    }
    String body = part.substring(1, part.length() - 1);
    int colon = body.indexOf(':');
    if (colon <= 0 || colon == body.length() - 1) {
      throw new MalformedExecutionIdException(raw, "segment is not [type:value]: " + part);
    }
    return new Segment(body.substring(0, colon), body.substring(colon + 1));
  }

  boolean isType(String expected) {
    return type.equals(expected);
  }

  boolean is(String expectedType, String expectedValue) {
    return isType(expectedType) && value.equals(expectedValue);
  }
}
