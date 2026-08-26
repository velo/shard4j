package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Grant;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The fence-blind-ledger regression, at the ledger boundary: a unit re-pooled mid-run
 * (coordinator restart, lease TTL expiry -- both supported events) is re-granted to a
 * sibling slot under a new fence while the first slot is still running it. The first
 * slot's report is stale, and explaining it away must not erase the live lease -- a
 * failure after that must still drain the live fence for a NACK back to the pool, never
 * abandon it to the TTL, and never let a silent drop reconcile into a clean exit.
 * {@link ConcurrentAbandonIT} exercises the drain-and-NACK path end to end against a
 * real coordinator.
 */
class StaleFenceLedgerTest {

  private static final String UNIT =
      "[engine:junit-jupiter]/[class:com.example.StaleFenceProbe]/[method:unit()]";
  private static final Fence STALE_FENCE = new Fence(1, 1, 1);
  private static final Fence LIVE_FENCE = new Fence(1, 2, 1);

  private final LeaseLedger ledger = new LeaseLedger();

  @Test
  void givenAUnitRegrantedUnderANewFence_whenItsStaleReportIsExplained_thenTheLiveLeaseIsStillDrained() {
    ledger.track(List.of(grant(STALE_FENCE)));
    ledger.track(List.of(grant(LIVE_FENCE)));

    ledger.explain(UNIT, STALE_FENCE);

    List<Grant> drained = ledger.drainAll();
    assertThat(drained).hasSize(1);
    assertThat(drained.get(0).testId()).isEqualTo(UNIT);
    assertThat(drained.get(0).fence()).isEqualTo(LIVE_FENCE);
  }

  @Test
  void givenALeaseStillUnderItsGrantedFence_whenItsReportIsExplained_thenNothingIsLeftToDrain() {
    ledger.track(List.of(grant(LIVE_FENCE)));

    ledger.explain(UNIT, LIVE_FENCE);

    assertThat(ledger.drainAll()).isEmpty();
  }

  @Test
  void givenADrainedLedger_whenDrainedAgain_thenTheSecondDrainIsANoOp() {
    ledger.track(List.of(grant(LIVE_FENCE)));

    assertThat(ledger.drainAll()).hasSize(1);
    assertThat(ledger.drainAll()).isEmpty();
  }

  private static Grant grant(Fence fence) {
    return new Grant(UNIT, fence, Instant.now().plusSeconds(60), false, 3);
  }
}
