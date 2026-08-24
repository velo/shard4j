package com.marvinformatics.shard4j.protocol;

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
    throw new UnsupportedOperationException("not implemented");
  }

  public Shape shape() {
    throw new UnsupportedOperationException("not implemented");
  }

  /** True for shapes A and B: the granularity a shard claims. */
  public boolean isLeaseUnit() {
    throw new UnsupportedOperationException("not implemented");
  }

  /** Drops the engine segments an outer engine prepends, leaving the wire form. */
  public static String stripOuterEngineSegment(String nestedId) {
    throw new UnsupportedOperationException("not implemented");
  }

  /** Re-prepends the outer engine segment when handing an id back to the platform. */
  public String withOuterEngineSegment(String engineId) {
    throw new UnsupportedOperationException("not implemented");
  }
}
