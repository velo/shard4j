package com.marvinformatics.shard4j.protocol;

/**
 * A Jupiter {@code UniqueId} in its wire form, rooted at {@code [engine:junit-jupiter]}.
 *
 * <p>Opaque at this boundary. The engine builds it from a live {@code TestDescriptor}
 * through JUnit's own {@code UniqueId} API, so its syntax is JUnit's to guarantee; the
 * coordinator stores and returns it verbatim. This type validates no syntax: doing so
 * caught nothing that can actually go wrong -- a stale invocation id is perfectly
 * well-formed -- and rejected forms JUnit really emits, such as a {@code [I} array
 * parameter type. The one place the claimable shapes are taken apart or put together is
 * {@link CensusUnit}.
 *
 * <p>Two granularities share this type and must not be collapsed: a lease unit -- a plain
 * method or a test-template container, the thing a shard claims -- and a record id -- what
 * a completed result is reported under, which may carry a trailing
 * {@code [test-template-invocation:#N]} segment.
 */
public record ExecutionId(String value) {

  public ExecutionId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("An execution id must not be blank");
    }
  }
}
