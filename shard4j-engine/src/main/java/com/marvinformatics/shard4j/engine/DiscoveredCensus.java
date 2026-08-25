package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId.Segment;

/**
 * The census: a pure discovery product, expressed in lease units -- one entry per plain
 * test method and one per test-template <em>method</em>, never per invocation, because
 * invocations do not exist at discovery. Built from the descriptor tree the launcher has
 * already pruned, so every filter the consumer configured -- includes, excludes, tags --
 * is inherited for free, and nothing permanently unclaimable can enter it.
 *
 * <p>Any other descriptor shape fails loudly naming the id. A shape this engine cannot
 * lease, silently dropped, is a test running nowhere behind a green build -- the exact
 * failure mode the census exists to delete.
 */
final class DiscoveredCensus {

  /** One claimable class: the coordinator's claim unit is a class-batch. */
  record ClassUnits(String className, List<ExecutionId> units) {}

  private final Map<String, ClassUnits> byClassName;

  private DiscoveredCensus(List<ClassUnits> classes) {
    Map<String, ClassUnits> byName = new LinkedHashMap<>();
    classes.forEach(entry -> byName.put(entry.className(), entry));
    this.byClassName = Collections.unmodifiableMap(byName);
  }

  static DiscoveredCensus of(TestDescriptor jupiterRoot) {
    List<ClassUnits> classes = new ArrayList<>();
    for (TestDescriptor classCandidate : jupiterRoot.getChildren()) {
      Segment segment = classCandidate.getUniqueId().getLastSegment();
      if (!"class".equals(segment.getType())) {
        throw failLoudly(classCandidate);
      }
      List<ExecutionId> units = new ArrayList<>();
      for (TestDescriptor member : classCandidate.getChildren()) {
        String type = member.getUniqueId().getLastSegment().getType();
        if (!"method".equals(type) && !"test-template".equals(type)) {
          throw failLoudly(member);
        }
        units.add(ExecutionIdentity.leaseId(member));
      }
      if (!units.isEmpty()) {
        classes.add(new ClassUnits(segment.getValue(), units));
      }
    }
    return new DiscoveredCensus(classes);
  }

  /** For tests that need a census discovery could never produce, such as a stale unit. */
  static DiscoveredCensus of(List<ClassUnits> classes) {
    return new DiscoveredCensus(classes);
  }

  private static IllegalStateException failLoudly(TestDescriptor descriptor) {
    return new IllegalStateException(
        "The census cannot express "
            + descriptor.getUniqueId()
            + " as a lease unit. Only plain test methods and test-template methods are"
            + " distributable; nested classes, test factories and other shapes must scream"
            + " here rather than silently run nowhere.");
  }

  /**
   * The units discovered for one class. Which class runs next is the coordinator's
   * decision, so this is a lookup rather than a sequence: a name the census does not hold
   * is a coordinator bug -- registration proved both sides agree on the census -- and it
   * fails here naming the class rather than resolving to nothing further down.
   */
  ClassUnits classNamed(String className) {
    ClassUnits entry = byClassName.get(className);
    if (entry == null) {
      throw new ShardExecutionException(
          "Granted a class this shard never registered: " + className);
    }
    return entry;
  }

  List<String> unitIds() {
    return byClassName.values().stream()
        .flatMap(entry -> entry.units().stream())
        .map(ExecutionId::value)
        .sorted()
        .toList();
  }

  boolean isEmpty() {
    return byClassName.isEmpty();
  }
}
