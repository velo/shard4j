package com.marvinformatics.shard4j.coordinator.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.CensusUnit;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The hold-back policy as pure logic. The declared fleet may size shares smaller, but
 * only a live shard can make the cap bind -- a declared shard that never registers must
 * never strand a unit behind an allowance nobody will ever claim.
 */
class FairShareTest {

  private static final Instant CREATED = Instant.parse("2026-08-20T10:00:00Z");
  private static final String TEMPLATE =
      "[engine:junit-jupiter]/[class:com.example.orders.OrderIT]"
          + "/[test-template:rows(java.lang.String)]";

  private final ShardRoster roster = new ShardRoster();

  private FairShare fairShare() {
    return new FairShare(roster, CREATED);
  }

  private void liveShard(int index) {
    roster.join(index, CREATED);
  }

  private static List<Session.UnitState> pendingUnits(int count) {
    List<Session.UnitState> units = new ArrayList<>();
    CensusUnit whole = CensusUnit.parse(TEMPLATE);
    for (int position = 1; position <= count; position++) {
      units.add(
          new Session.UnitState(
              new ClaimableUnit(TEMPLATE, whole.atPosition(position), false)));
    }
    return units;
  }

  @Test
  void givenDeclaredShardsThatNeverRegistered_whenTheOnlyLiveShardAsks_thenTheCapDoesNotBind() {
    liveShard(0);
    FairShare share = fairShare();
    share.declareFleet(4);
    int allowance = share.invocationAllowance(pendingUnits(5), 0, CREATED);
    assertThat(allowance).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void givenAnotherLiveShardStillWorking_whenAsking_thenTheDeclaredFleetSizesTheShare() {
    liveShard(0);
    liveShard(1);
    FairShare share = fairShare();
    share.declareFleet(4);
    // Ceil of eight eligible over the declared fleet of four: room is left for the two
    // shards still booting, because a live asker can still come back for the remainder.
    assertThat(share.invocationAllowance(pendingUnits(8), 0, CREATED)).isEqualTo(2);
  }

  @Test
  void givenTheOtherLiveShardExhausted_whenTheLastAskerAsks_thenItIsNeverCapped() {
    liveShard(0);
    liveShard(1);
    FairShare share = fairShare();
    share.declareFleet(4);
    share.markExhausted(1);
    assertThat(share.invocationAllowance(pendingUnits(8), 0, CREATED))
        .isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void givenTheArrivalWindowClosed_whenAsking_thenSharesAreSizedByTheLiveRosterAlone() {
    liveShard(0);
    liveShard(1);
    FairShare share = fairShare();
    share.declareFleet(4);
    Instant afterWindow = CREATED.plus(FairShare.FLEET_ARRIVAL_WINDOW).plus(Duration.ofSeconds(1));
    assertThat(share.invocationAllowance(pendingUnits(8), 0, afterWindow)).isEqualTo(4);
  }
}
