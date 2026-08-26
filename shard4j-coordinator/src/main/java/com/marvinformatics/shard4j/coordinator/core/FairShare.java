package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.TestState;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The fair-share hold-back policy for a distributed method's invocations: a pure
 * computation over one method's expanded units, the live roster and the clock -- plus the
 * two facts only this policy cares about, the consumer-declared fleet size and which shards
 * have exhausted their open ask. {@link Session} owns the state machine and
 * {@link ShardRoster} owns liveness; this type answers exactly one question: how many more
 * of the method's invocations may this shard lease right now.
 */
final class FairShare {

  // How long after session creation the declared fleet size still widens share sizing,
  // so early askers leave room for shards that are booting. It sizes shares only: whether
  // the cap binds at all is decided by live shards alone, so a declared shard that never
  // boots can never strand a unit.
  static final Duration FLEET_ARRIVAL_WINDOW = Duration.ofSeconds(60);

  private final ShardRoster roster;
  private final Instant createdAt;
  private final Set<Integer> exhausted = new HashSet<>();
  private int declaredShardCount;

  FairShare(ShardRoster roster, Instant createdAt) {
    this.roster = roster;
    this.createdAt = createdAt;
  }

  /** The consumer-declared fleet size, kept as the maximum any registration reported. */
  void declareFleet(Integer shardCount) {
    if (shardCount != null && shardCount > declaredShardCount) {
      declaredShardCount = shardCount;
    }
  }

  /** The open ask came back empty for this shard: it has stopped pulling. */
  void markExhausted(int shard) {
    exhausted.add(shard);
  }

  /** Taking work ends the exhaustion, exactly as it ends the idle clock. */
  void resumed(int shard) {
    exhausted.remove(shard);
  }

  /** A new attempt's shards all ask afresh; no exhaustion survives the epoch bump. */
  void epochBumped() {
    exhausted.clear();
  }

  /**
   * How many more of the method's invocations this shard may lease right now. The cap is
   * a fair share -- ceil of the eligible invocations over the expected fleet --
   * and it binds only while another <em>live</em> shard may still ask: registered, not
   * departed, not released, and not exhausted. The declared
   * {@code shard.count} deliberately cannot make the cap bind: a shard that dies before
   * registration would otherwise hold invocations back forever -- the live shards drain
   * their shares, exhaust, stop pulling, and the remainder sits PENDING into an
   * INCOMPLETE verdict. The last live asker is never capped, which is what makes the
   * hold-back safe: spreading degrades to whole-method behaviour rather than stranding a
   * unit.
   */
  int invocationAllowance(List<Session.UnitState> units, int shard, Instant now) {
    if (!othersMayStillClaim(shard)) {
      return Integer.MAX_VALUE;
    }
    int eligible = 0;
    int mine = 0;
    for (Session.UnitState unit : units) {
      if (unit.state == TestState.LEASED) {
        eligible++;
        if (unit.lease.shard() == shard) {
          mine++;
        }
        continue;
      }
      if (Session.isClaimable(unit)) {
        eligible++;
        continue;
      }
      // Absorbed units still count toward the denominator: the share this shard has
      // already taken of a method's invocations is what makes the next ask fair. With
      // passes gone, "already run" is session-wide rather than per-pass.
      //
      // A unit is charged to every shard that ran it, not merely to the last one. Retries
      // are what makes those differ: a shard that failed an invocation another shard then
      // passed did consume a hand-out of this method, and un-charging it would let it take
      // the next one straight back -- which is exactly the spreading this cap exists to
      // enforce. The two counts are per shard and never compared to each other, so
      // charging one unit to two shards is well-defined.
      if (unit.records.isEmpty()) {
        continue;
      }
      eligible++;
      if (unit.records.stream().anyMatch(record -> record.shard() == shard)) {
        mine++;
      }
    }
    int share = Math.ceilDiv(eligible, expectedFleet(now));
    return Math.max(0, share - mine);
  }

  private int expectedFleet(Instant now) {
    int fleet = Math.max(1, roster.live().size());
    if (withinArrivalWindow(now)) {
      fleet = Math.max(fleet, declaredShardCount);
    }
    return fleet;
  }

  private boolean othersMayStillClaim(int shard) {
    return roster.live().stream()
        .anyMatch(other -> other != shard && !exhausted.contains(other));
  }

  private boolean withinArrivalWindow(Instant now) {
    return now.isBefore(createdAt.plus(FLEET_ARRIVAL_WINDOW));
  }
}
