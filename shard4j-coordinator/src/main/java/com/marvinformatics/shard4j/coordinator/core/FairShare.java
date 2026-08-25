package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The fair-share hold-back policy for a distributed method's invocations: a pure
 * computation over one method's expanded units, the roster view, the pass and the clock
 * -- plus the two facts only this policy cares about, the consumer-declared fleet size
 * and which shards have exhausted their open ask. The session owns the state machine and
 * the roster; this type answers exactly one question: how many more of the method's
 * invocations may this shard lease right now.
 */
final class FairShare {

  // How long after session creation a declared-but-unseen shard still reserves its fair
  // share of a template's invocations. Bounded so a shard that never boots costs at most
  // this much held-back spreading, not a stranded pass: once the window closes, whoever
  // is still asking takes the remainder.
  static final Duration FLEET_ARRIVAL_WINDOW = Duration.ofSeconds(60);

  private final Map<Integer, Session.ShardInfo> roster;
  private final Instant createdAt;
  private final Map<Integer, Pass> exhausted = new HashMap<>();
  private int declaredShardCount;

  FairShare(Map<Integer, Session.ShardInfo> roster, Instant createdAt) {
    this.roster = roster;
    this.createdAt = createdAt;
  }

  /** The consumer-declared fleet size, kept as the maximum any registration reported. */
  void declareFleet(Integer shardCount) {
    if (shardCount != null && shardCount > declaredShardCount) {
      declaredShardCount = shardCount;
    }
  }

  /** The open ask came back empty for this shard: it will not ask again in this pass. */
  void markExhausted(int shard, Pass pass) {
    exhausted.put(shard, pass);
  }

  /** A new attempt's shards all ask afresh; no exhaustion survives the epoch bump. */
  void epochBumped() {
    exhausted.clear();
  }

  /**
   * How many more of the method's invocations this shard may lease right now. The cap is
   * a fair share -- ceil of the pass's eligible invocations over the expected fleet --
   * and it only binds while some other shard may still ask: another live shard is still
   * working the pass, or a declared {@code shard.count} shard has not arrived and the
   * arrival window is open. The last shard still asking is never capped, which is what
   * makes the hold-back safe: spreading degrades to whole-method behaviour rather than
   * stranding a unit.
   */
  int invocationAllowance(List<Session.UnitState> units, int shard, Pass pass, Instant now) {
    if (!othersMayStillClaim(shard, pass, now)) {
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
      if (Session.claimableIn(unit, pass)) {
        eligible++;
        continue;
      }
      SessionView.RecordView latest =
          unit.records.isEmpty() ? null : unit.records.get(unit.records.size() - 1);
      if (latest != null && latest.pass() == pass) {
        eligible++;
        if (latest.shard() == shard) {
          mine++;
        }
      }
    }
    int share = Math.ceilDiv(eligible, expectedFleet(pass, now));
    return Math.max(0, share - mine);
  }

  private int expectedFleet(Pass pass, Instant now) {
    int active =
        (int) roster.values().stream().filter(info -> !info.departed && !info.released).count();
    int fleet = Math.max(1, active);
    if (pass == Pass.MAIN && withinArrivalWindow(now)) {
      fleet = Math.max(fleet, declaredShardCount);
    }
    return fleet;
  }

  private boolean othersMayStillClaim(int shard, Pass pass, Instant now) {
    boolean otherStillWorking =
        roster.entrySet().stream()
            .anyMatch(
                entry ->
                    entry.getKey() != shard
                        && !entry.getValue().departed
                        && !entry.getValue().released
                        && exhausted.get(entry.getKey()) != pass
                        && (entry.getValue().completedPass == null
                            || entry.getValue().completedPass.ordinal() < pass.ordinal()));
    if (otherStillWorking) {
      return true;
    }
    return pass == Pass.MAIN && withinArrivalWindow(now) && declaredShardCount > roster.size();
  }

  private boolean withinArrivalWindow(Instant now) {
    return now.isBefore(createdAt.plus(FLEET_ARRIVAL_WINDOW));
  }
}
