package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.HistoryKey;

/**
 * A census unit as the coordinator holds it: the wire id, plus the facts derived from it
 * once, when the census is registered.
 *
 * <p>The protocol calls an execution id opaque, and it stays that way -- the single
 * derivation lives in {@link HistoryKeys}, and everything downstream reads these fields
 * instead of parsing the id again. That is what lets the scheduler both rank the pool and
 * group it by class without a second grammar to keep in step with the first.
 *
 * <p>{@code template} says the unit is a test-template method rather than a plain one --
 * the only shape whose invocations can be handed out individually. {@code invocation} is
 * the positional index ({@code #N}) when the id is a single invocation of a template, and
 * null for a whole method or whole template. The history key stays at method level in
 * every case: distribution acts on positions, storage never does.
 */
public record CensusUnit(
    String id, String className, HistoryKey historyKey, boolean template, String invocation) {}
