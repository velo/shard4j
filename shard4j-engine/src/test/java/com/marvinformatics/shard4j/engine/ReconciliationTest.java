package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.Pass;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The three-way classification and its wording, without a container. */
class ReconciliationTest {

  private static final String TEMPLATE =
      "[engine:junit-jupiter]/[class:com.example.orders.OrderIT]"
          + "/[test-template:rows(java.lang.String)]";
  private static final String WHOLE_METHOD =
      "[engine:junit-jupiter]/[class:com.example.orders.OrderIT]/[method:plain()]";

  private static Grant grant(String testId, boolean probe) {
    return new Grant(testId, new Fence(1, 1, 1), Instant.parse("2026-08-20T10:00:00Z"), probe);
  }

  private static String invocation(int position) {
    return TEMPLATE + "/[test-template-invocation:#" + position + "]";
  }

  @Test
  void givenOnlyAVanishedProbe_whenClassified_thenItIsNackedVanishedAndNothingFails() {
    Reconciliation reconciliation =
        Reconciliation.classify(List.of(grant(invocation(5), true)), 1, Pass.MAIN);
    assertThat(reconciliation.failure()).isNull();
    assertThat(reconciliation.nacks()).hasSize(1);
    NackRequest.NackedLease nack = reconciliation.nacks().get(0);
    assertThat(nack.vanished()).isTrue();
    assertThat(nack.reason())
        .contains("Cardinality probe past recorded history")
        .contains("the recorded parameter count still stands");
  }

  @Test
  void givenAVanishedMeasuredInvocation_whenClassified_thenItIsNackedVanishedAndTheFailureNamesTheDrift() {
    Reconciliation reconciliation =
        Reconciliation.classify(List.of(grant(invocation(4), false)), 0, Pass.MAIN);
    assertThat(reconciliation.nacks()).hasSize(1);
    assertThat(reconciliation.nacks().get(0).vanished()).isTrue();
    assertThat(reconciliation.nacks().get(0).reason())
        .contains("the parameter set changed since this invocation was last measured");
    assertThat(reconciliation.failure())
        .contains("the parameter set changed since they were last measured")
        .contains(invocation(4));
  }

  @Test
  void givenAWholeUnit_whenClassified_thenItIsNackedBackToThePoolAndTheFailureBlamesTheEngine() {
    Reconciliation reconciliation =
        Reconciliation.classify(List.of(grant(WHOLE_METHOD, false)), 2, Pass.RETRY1);
    assertThat(reconciliation.nacks()).hasSize(1);
    assertThat(reconciliation.nacks().get(0).vanished()).isFalse();
    assertThat(reconciliation.nacks().get(0).reason())
        .contains("Leased but never produced a terminal outcome");
    assertThat(reconciliation.failure())
        .contains("could not reconcile 1 leased unit(s)")
        .contains(WHOLE_METHOD);
  }

  @Test
  void givenDriftAndAnUnexplainedUnitTogether_whenClassified_thenOneMessageNamesBoth() {
    Reconciliation reconciliation =
        Reconciliation.classify(
            List.of(grant(invocation(3), false), grant(WHOLE_METHOD, false)), 0, Pass.MAIN);
    assertThat(reconciliation.failure())
        .contains("the parameter set changed since they were last measured")
        .contains(". It also")
        .contains("could not reconcile");
  }

  @Test
  void givenAbandonedLeases_whenWorded_thenEveryNackNamesTheCauseAndReturnsToThePool() {
    List<NackRequest.NackedLease> nacks =
        Reconciliation.abandoned(
            List.of(grant(WHOLE_METHOD, false), grant(invocation(1), false)),
            1,
            Pass.MAIN,
            "the shard JVM was terminated mid-pass");
    assertThat(nacks).hasSize(2);
    assertThat(nacks)
        .allSatisfy(
            nack -> {
              assertThat(nack.vanished()).isFalse();
              assertThat(nack.reason())
                  .contains("Abandoned on shard 1")
                  .contains("the shard JVM was terminated mid-pass")
                  .contains("returned to the pool");
            });
  }
}
