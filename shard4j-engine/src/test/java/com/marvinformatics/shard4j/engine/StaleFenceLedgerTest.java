package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.Pass;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.UniqueId;

/**
 * The fence-blind-ledger regression: a unit re-pooled mid-run (coordinator restart, lease
 * TTL expiry -- both supported events) is re-granted to a sibling slot under a new fence
 * while the first slot is still running it. The first slot's report is stale, and
 * explaining it away must not erase the live lease from the reconciliation ledger -- when
 * the shard then fails, the live fence must be NACKed back to the pool, never abandoned
 * to the TTL, and never left for a silent drop to reconcile into a clean exit.
 *
 * <p>Scripted gateway rather than a real coordinator, as a last resort: the trigger is an
 * exact interleaving -- the re-grant landing after the first slot's claim and before its
 * report -- that no healthy coordinator can be asked to produce deterministically.
 * {@link ConcurrentAbandonIT} covers the same failure path against a real coordinator,
 * but its slots never collide on one unit id, which is the whole hazard here.
 */
class StaleFenceLedgerTest {

  private static final String CLASS_NAME = StaleFenceFixture.class.getName();
  private static final String UNIT =
      "[engine:junit-jupiter]/[class:" + CLASS_NAME + "]/[method:unit()]";
  private static final Fence STALE_FENCE = new Fence(1, 1, 1);
  private static final Fence LIVE_FENCE = new Fence(1, 2, 1);

  private final List<Fence> reportedFences = Collections.synchronizedList(new ArrayList<>());
  private final List<NackRequest.NackedLease> nacks =
      Collections.synchronizedList(new ArrayList<>());
  private final CountDownLatch staleReported = new CountDownLatch(1);
  private final AtomicInteger asks = new AtomicInteger();
  private final AtomicInteger claims = new AtomicInteger();

  @Test
  void givenAUnitRegrantedToASiblingSlotUnderANewFence_whenItsStaleReportLands_thenTheLiveLeaseIsStillNackedOnFailure() {
    StaleFenceFixture.release = new CountDownLatch(1);
    ShardConfiguration configuration =
        new ShardConfiguration(
            true,
            "http://localhost:1",
            "unused",
            "stale-fence-session",
            0,
            Pass.MAIN,
            1,
            2,
            Map.of(),
            Duration.ofSeconds(5),
            null,
            true);
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(new DiscoveredCensus.ClassUnits(CLASS_NAME, List.of(new ExecutionId(UNIT)))));
    ShardLoop loop =
        new ShardLoop(
            configuration,
            new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
            scriptedGateway(configuration, census),
            EngineTestHarness.outerRequest(EngineExecutionListener.NOOP));

    assertThatThrownBy(() -> loop.run(census))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("simulated transport failure");

    // The first slot really did report under the reclaimed fence, and only under it.
    assertThat(reportedFences).containsExactly(STALE_FENCE);
    // The live lease survived the stale report's explanation and was NACKed, so a
    // transport death returns the unit to the pool now instead of leasing it out the TTL.
    assertThat(nacks).hasSize(1);
    assertThat(nacks.get(0).testId()).isEqualTo(UNIT);
    assertThat(nacks.get(0).fence()).isEqualTo(LIVE_FENCE);
    assertThat(nacks.get(0).reason()).contains("Abandoned");
  }

  /**
   * The script, with the dispatch lock serialising the two slots: the first ask hands out
   * the unit under the stale fence and its slot blocks inside the fixture; the second ask
   * re-grants the same unit under the live fence -- the mid-run re-pool -- and its claim
   * releases the fixture, waits for the stale report to land, then dies.
   */
  private CoordinatorGateway scriptedGateway(
      ShardConfiguration configuration, DiscoveredCensus census) {
    return new CoordinatorGateway(configuration, census.unitIds()) {
      @Override
      long register() {
        return 0;
      }

      @Override
      void keepalive() {}

      @Override
      NextClassResponse nextClass() {
        return switch (asks.incrementAndGet()) {
          case 1 -> new NextClassResponse(CLASS_NAME, List.of(grant(STALE_FENCE)));
          case 2 -> new NextClassResponse(CLASS_NAME, List.of(grant(LIVE_FENCE)));
          default -> new NextClassResponse(null, List.of());
        };
      }

      @Override
      List<Grant> claim(String className, List<String> candidates) {
        if (claims.incrementAndGet() == 1) {
          return List.of();
        }
        StaleFenceFixture.release.countDown();
        awaitStaleReport();
        throw new IllegalStateException("simulated transport failure: retry budget spent");
      }

      @Override
      void report(Fence fence, UnitResult result, boolean firstOnShard) {
        reportedFences.add(fence);
        if (fence.equals(STALE_FENCE)) {
          staleReported.countDown();
        }
      }

      @Override
      void nack(List<NackRequest.NackedLease> leases) {
        nacks.addAll(leases);
      }
    };
  }

  private void awaitStaleReport() {
    try {
      if (!staleReported.await(20, TimeUnit.SECONDS)) {
        throw new IllegalStateException("The stale report never landed");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static Grant grant(Fence fence) {
    return new Grant(UNIT, fence, Instant.now().plusSeconds(60));
  }
}
