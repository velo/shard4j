package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.HistoryKey;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.UniqueId.Segment;
import org.junit.platform.engine.support.descriptor.MethodSource;

/**
 * Builds the protocol's identity values from live JUnit Platform objects. This is the only
 * place either value is constructed: everything downstream -- the wire, the coordinator's
 * store, the duration history -- treats them as opaque strings.
 */
@UtilityClass
public class ExecutionIdentity {

  private final String ENGINE_SEGMENT = "engine";
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
    if (wire.getLastSegment().getType().equals(INVOCATION_SEGMENT)) {
      wire = wire.removeLastSegment();
    }
    return new ExecutionId(wire.toString());
  }

  /**
   * The duration-history key, from the method source's three fields. Invocations of one
   * template share the template's source, so they collapse onto its key with no id
   * surgery.
   */
  public HistoryKey historyKey(TestDescriptor descriptor) {
    TestSource source = descriptor.getSource().orElse(null);
    if (!(source instanceof MethodSource method)) {
      throw new IllegalArgumentException(
          "No method source on " + descriptor.getUniqueId() + ", so no history key exists");
    }
    return new HistoryKey(
        method.getClassName()
            + "#"
            + method.getMethodName()
            + "("
            + method.getMethodParameterTypes()
            + ")");
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
        && segments.get(root).getType().equals(ENGINE_SEGMENT)
        && !segments.get(root).getValue().equals(JUPITER_ENGINE_ID)) {
      root++;
    }
    if (root == segments.size() || !segments.get(root).getType().equals(ENGINE_SEGMENT)) {
      throw new IllegalArgumentException("Not a Jupiter-rooted unique id: " + uniqueId);
    }
    UniqueId wire = UniqueId.forEngine(JUPITER_ENGINE_ID);
    for (Segment segment : segments.subList(root + 1, segments.size())) {
      wire = wire.append(segment);
    }
    return wire;
  }
}
