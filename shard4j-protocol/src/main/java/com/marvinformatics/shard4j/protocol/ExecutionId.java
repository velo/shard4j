package com.marvinformatics.shard4j.protocol;

import java.util.List;
import java.util.regex.Pattern;

/**
 * A raw Jupiter {@code UniqueId} in its wire form, always rooted at
 * {@code [engine:junit-jupiter]}. Three shapes matter:
 *
 * <pre>
 * A  [engine:junit-jupiter]/[class:&lt;FQCN&gt;]/[method:&lt;name&gt;(&lt;paramTypes&gt;)]
 * B  [engine:junit-jupiter]/[class:&lt;FQCN&gt;]/[test-template:&lt;name&gt;(&lt;paramTypes&gt;)]
 * C  B + /[test-template-invocation:#&lt;N&gt;]
 * </pre>
 *
 * <p>A lease unit -- the queue key, the lease key, the thing a shard claims -- is A or B.
 * A record id -- what a completed result is reported under -- is A or C. The two are not
 * the same granularity and must not be collapsed.
 */
public record ExecutionId(String value) {

  private static final String JUPITER_ENGINE = "junit-jupiter";

  private static final String IDENTIFIER = "[A-Za-z_$][A-Za-z0-9_$]*";
  private static final String TYPE_NAME = IDENTIFIER + "(?:\\." + IDENTIFIER + ")*(?:\\[\\])*";
  private static final Pattern FQCN = Pattern.compile(TYPE_NAME);
  private static final Pattern SIGNATURE =
      Pattern.compile(IDENTIFIER + "\\((?:" + TYPE_NAME + "(?:, " + TYPE_NAME + ")*)?\\)");
  private static final Pattern INVOCATION_INDEX = Pattern.compile("#[1-9][0-9]*");
  private static final Pattern ENGINE_ID = Pattern.compile("[^\\[\\]:/\\s]+");

  private static final String JUPITER_ROOT = "[engine:" + JUPITER_ENGINE + "]";

  /** The shape of an execution id, which decides where it may legally appear. */
  public enum Shape {
    METHOD,
    TEST_TEMPLATE,
    TEST_TEMPLATE_INVOCATION
  }

  /**
   * Parses and validates against the grammar above. An id of any other shape (a nested
   * class, a test factory, a repeated test) must fail loudly naming the id, never be
   * silently dropped.
   */
  public static ExecutionId parse(String raw) {
    if (raw == null) {
      throw new MalformedExecutionIdException("null", "no id was supplied");
    }
    shapeOf(Segment.split(raw), raw);
    return new ExecutionId(raw);
  }

  public Shape shape() {
    return shapeOf(Segment.split(value), value);
  }

  /** True for shapes A and B: the granularity a shard claims. */
  public boolean isLeaseUnit() {
    return shape() != Shape.TEST_TEMPLATE_INVOCATION;
  }

  /** The {@code [class:...]} segment's value: a fully qualified class name. */
  String className() {
    return Segment.split(value).get(1).value();
  }

  /** The lease unit's {@code <name>(<paramTypes>)} signature, invocation index dropped. */
  String unitSignature() {
    return Segment.split(value).get(2).value();
  }

  private static Shape shapeOf(List<Segment> segments, String raw) {
    if (segments.size() < 3
        || !segments.get(0).is("engine", JUPITER_ENGINE)
        || !segments.get(1).isType("class")) {
      throw new MalformedExecutionIdException(raw, "must start with [engine:junit-jupiter]/[class:");
    }
    require(FQCN, segments.get(1).value(), raw, "not a fully qualified class name");

    Segment unit = segments.get(2);
    if (unit.isType("method") && segments.size() == 3) {
      require(SIGNATURE, unit.value(), raw, "not a <name>(<paramTypes>) signature");
      return Shape.METHOD;
    }
    if (unit.isType("test-template")) {
      require(SIGNATURE, unit.value(), raw, "not a <name>(<paramTypes>) signature");
      if (segments.size() == 3) {
        return Shape.TEST_TEMPLATE;
      }
      if (segments.size() == 4 && segments.get(3).isType("test-template-invocation")) {
        require(INVOCATION_INDEX, segments.get(3).value(), raw, "not a 1-based #<N> index");
        return Shape.TEST_TEMPLATE_INVOCATION;
      }
    }
    throw new MalformedExecutionIdException(raw, "unsupported shape");
  }

  private static void require(Pattern pattern, String value, String raw, String reason) {
    if (!pattern.matcher(value).matches()) {
      throw new MalformedExecutionIdException(raw, reason + ": " + value);
    }
  }

  /** Drops the engine segments an outer engine prepends, leaving the wire form. */
  public static String stripOuterEngineSegment(String nestedId) {
    if (nestedId == null) {
      throw new MalformedExecutionIdException("null", "no id was supplied");
    }
    int jupiter = nestedId.indexOf(JUPITER_ROOT);
    if (jupiter < 0 || (jupiter > 0 && nestedId.charAt(jupiter - 1) != '/')) {
      throw new MalformedExecutionIdException(nestedId, "no " + JUPITER_ROOT + " segment");
    }
    return nestedId.substring(jupiter);
  }

  /** Re-prepends the outer engine segment when handing an id back to the platform. */
  public String withOuterEngineSegment(String engineId) {
    if (engineId == null || !ENGINE_ID.matcher(engineId).matches()) {
      throw new IllegalArgumentException("Not a usable engine id: " + engineId);
    }
    return "[engine:" + engineId + "]/" + value;
  }
}
