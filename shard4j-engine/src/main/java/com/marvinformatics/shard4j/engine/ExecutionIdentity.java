package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.UniqueId.Segment;

/**
 * Builds the protocol's execution ids from live JUnit Platform objects. This is the only
 * place the value is constructed: everything downstream -- the wire, the coordinator's
 * store, the duration history -- treats it as an opaque string.
 */
@UtilityClass
public class ExecutionIdentity {

  private final String ENGINE_SEGMENT = "engine";
  private final String CLASS_SEGMENT = "class";
  private final String JUPITER_ENGINE_ID = "junit-jupiter";
  private final String INVOCATION_SEGMENT = "test-template-invocation";

  /** The record id: the Jupiter-rooted wire form of the descriptor's unique id. */
  public ExecutionId executionId(TestDescriptor descriptor) {
    return executionId(descriptor.getUniqueId());
  }

  /** As above, from a unique id that outer engines may have nested under their roots. */
  public ExecutionId executionId(UniqueId uniqueId) {
    return new ExecutionId(jupiterRooted(uniqueId).toString());
  }

  /** The lease unit: the record id with a trailing invocation segment dropped. */
  public ExecutionId leaseId(TestDescriptor descriptor) {
    UniqueId wire = jupiterRooted(descriptor.getUniqueId());
    if (INVOCATION_SEGMENT.equals(wire.getLastSegment().getType())) {
      wire = wire.removeLastSegment();
    }
    return new ExecutionId(wire.toString());
  }

  /** Re-roots a wire-form id under this engine's own root when handing it back to the platform. */
  public UniqueId underEngineRoot(UniqueId engineRoot, ExecutionId id) {
    UniqueId result = engineRoot;
    for (Segment segment : UniqueId.parse(id.value()).getSegments()) {
      result = result.append(segment);
    }
    return result;
  }

  /** Drops the engine segments an outer engine prepends, leaving the Jupiter-rooted form. */
  private UniqueId jupiterRooted(UniqueId uniqueId) {
    List<Segment> segments = uniqueId.getSegments();
    int root = 0;
    while (root < segments.size()
        && ENGINE_SEGMENT.equals(segments.get(root).getType())
        && !JUPITER_ENGINE_ID.equals(segments.get(root).getValue())) {
      root++;
    }
    if (root == segments.size() || !ENGINE_SEGMENT.equals(segments.get(root).getType())) {
      throw new IllegalArgumentException("Not a Jupiter-rooted unique id: " + uniqueId);
    }
    // The wire contract: after the engine root comes a class segment or nothing at all
    // (the engine node itself). Anything else is not an id this engine ever minted.
    if (root + 1 < segments.size() && !CLASS_SEGMENT.equals(segments.get(root + 1).getType())) {
      throw new IllegalArgumentException(
          "A Jupiter-rooted id must start with [class: after its engine root: " + uniqueId);
    }
    UniqueId wire = UniqueId.forEngine(JUPITER_ENGINE_ID);
    for (Segment segment : segments.subList(root + 1, segments.size())) {
      wire = wire.append(segment);
    }
    return wire;
  }
}
