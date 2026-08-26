package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.SessionView;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The shard roster, and every policy that reads nothing but the roster: liveness, the idle
 * clock, early release and the hunger order. The session owns the per-unit state machine
 * and delegates the whole of a shard's bookkeeping here, so no other type touches a
 * {@link ShardInfo} field directly.
 *
 * <p>The idle clock is why this is one type rather than six call sites. It is started in
 * exactly one place -- {@link #markIdle} -- but several unrelated signals end it, because
 * several unrelated signals are proof that a shard went back to work: taking a lease,
 * completing a unit, NACKing one, reporting a stale result. Spread across the session
 * those endings drifted apart, and a missed one leaves a working shard looking starved,
 * which outranks the genuine waiters and releases shards the run still needs.
 * {@link #proofOfWork} is the single ending.
 */
final class ShardRoster {

  /** The poll cadence the design fixes for a shard waiting at a barrier. */
  static final int RETRY_AFTER_SECONDS = 5;

  // The cadence is mandated, so silence is measurable: a shard holding no lease that has
  // missed three consecutive polls is presumed dead. Generous against jitter, and cheap to
  // be wrong about -- a merely slow or partitioned shard rejoins on its next call.
  private static final Duration PRESUMED_DEAD_AFTER =
      Duration.ofSeconds(3L * RETRY_AFTER_SECONDS);

  private final Map<Integer, ShardInfo> shards = new TreeMap<>();

  void join(int shard, Instant now) {
    ShardInfo info = infoOf(shard);
    info.departed = false;
    info.explicitlyDeparted = false;
    info.lastSeenAt = now;
  }

  /** The shard's own goodbye -- unlike a presumed death, a barrier packet cannot undo it. */
  void depart(int shard) {
    ShardInfo info = infoOf(shard);
    info.departed = true;
    info.explicitlyDeparted = true;
  }

  boolean hasJoined(int shard) {
    ShardInfo info = shards.get(shard);
    return info != null && !info.departed;
  }

  boolean hasDeparted(int shard) {
    ShardInfo info = shards.get(shard);
    return info != null && info.departed;
  }

  boolean isReleased(int shard) {
    ShardInfo info = shards.get(shard);
    return info != null && info.released;
  }

  void release(int shard) {
    infoOf(shard).released = true;
  }

  boolean isIdle(int shard) {
    ShardInfo info = shards.get(shard);
    return info != null && info.idleSince != null;
  }

  /** Shards that could still take work: registered, not departed, not released. */
  Set<Integer> live() {
    Set<Integer> live = new LinkedHashSet<>();
    shards.forEach(
        (shard, info) -> {
          if (!info.departed && !info.released) {
            live.add(shard);
          }
        });
    return live;
  }

  /**
   * Arrival at a barrier is proof of life, so it reverses a presumed death -- and that
   * revival is load-bearing: were a fleet's leases all to expire at once, the pools would
   * be resolved against zero live shards and every still-working shard would be released
   * with units pending. An explicit departure is different: the shard said goodbye, so the
   * only arrival that can follow is a delayed or duplicated packet, and reviving on that
   * would resurrect a shard that will never poll again into every future quorum.
   */
  void markIdle(int shard, Instant now) {
    ShardInfo info = infoOf(shard);
    if (!info.explicitlyDeparted) {
      info.departed = false;
      info.lastSeenAt = now;
    }
    // First arrival wins: the clock measures how long this shard has been starved, and
    // restarting it on every poll would make a patient shard look freshly idle forever.
    if (info.idleSince == null) {
      info.idleSince = now;
    }
  }

  /**
   * The single ending of the idle clock, for every signal that proves a shard was holding
   * or running work rather than waiting for some.
   *
   * <p>Deliberately does not enrol an unknown shard. The signals that reach here can name
   * a shard the roster has never seen -- a NACK or a stale result from a zombie -- and an
   * entry created for one would carry no {@code lastSeenAt}, which {@link #departSilent}
   * reads as a shard that has been silent forever and departs on the spot.
   *
   * <p>Only the in-memory clock is ended, so a restart re-folds the log and can bring back
   * an {@code idleSince} the live run had cleared. Every ending whose signal <em>is</em>
   * logged -- completions and NACKs -- therefore replays too, which narrows the window to
   * "restart until that shard's next lease" rather than closing it: a shard that idles,
   * leases and dies before producing either leaves nothing in the log to clear it. The cost
   * is a skew in the hunger order rather than lost work, since ranks among the live idle
   * shards stay a permutation and the sizes of the RUN/WAIT/DONE bands do not move.
   * Closing it properly means logging leases, which is a deliberate non-goal.
   */
  void proofOfWork(int shard) {
    ShardInfo info = shards.get(shard);
    if (info != null) {
      info.idleSince = null;
    }
  }

  /** A completed unit: proof of work, one more completion, and a fresh liveness stamp. */
  void recordCompletion(int shard, Instant now) {
    ShardInfo info = infoOf(shard);
    info.idleSince = null;
    info.completed++;
    info.lastSeenAt = now;
  }

  /**
   * The previous attempt's shards are known-dead and its barriers never resolve; the new
   * attempt's shards re-join with a clean watermark and no early release.
   */
  void epochBumped() {
    for (ShardInfo info : shards.values()) {
      info.departed = true;
      info.explicitlyDeparted = false;
      info.idleSince = null;
      info.released = false;
    }
  }

  /** Marks a shard dead on someone else's evidence; true when this call was the departure. */
  boolean presumeDead(int shard) {
    ShardInfo info = infoOf(shard);
    if (info.departed) {
      return false;
    }
    info.departed = true;
    return true;
  }

  /**
   * The waiter-side twin of lease expiry: a shard waiting at a barrier holds no lease, so
   * expiry can never notice its death, yet its stale watermark would keep counting in the
   * waiter tally and the quorum forever. The mandated poll cadence makes its silence
   * measurable instead -- no lease held and nothing heard for {@link #PRESUMED_DEAD_AFTER}
   * means presumed dead, and it departs exactly as an expired holder does. Released shards
   * are exempt: DONE told them to stop polling, so silence is their normal state, and they
   * already count in no quorum and no tally.
   */
  List<Integer> departSilent(Instant now, Set<Integer> leaseHolders) {
    List<Integer> newlyDeparted = new ArrayList<>();
    shards.forEach(
        (shard, info) -> {
          boolean silentTooLong =
              info.lastSeenAt == null || !info.lastSeenAt.plus(PRESUMED_DEAD_AFTER).isAfter(now);
          if (!info.departed
              && !info.released
              && silentTooLong
              && !leaseHolders.contains(shard)) {
            info.departed = true;
            newlyDeparted.add(shard);
          }
        });
    return newlyDeparted;
  }

  /**
   * How many live shards have been starved longer than this one. Polling is pull-based, so
   * without an explicit order a requeued unit goes to whoever happens to ask next -- which
   * on a fleet of equal pollers is arbitrary, and reliably starves a shard that has been
   * waiting since long before the others arrived. Ties break on shard index so the order is
   * total and no two shards ever read the same rank.
   *
   * <p>The barrier records the asker's arrival before asking for a decision, so in practice
   * the asker always has a watermark. A shard without one has been starved for no time at
   * all, which ranks it behind every shard that has actually waited.
   */
  int hungerRank(int shard) {
    ShardInfo me = shards.get(shard);
    Instant mine = me == null || me.idleSince == null ? Instant.MAX : me.idleSince;
    return (int)
        shards.entrySet().stream()
            .filter(entry -> entry.getKey() != shard)
            .filter(entry -> !entry.getValue().departed && !entry.getValue().released)
            .filter(entry -> entry.getValue().idleSince != null)
            .filter(
                entry -> {
                  int byTime = entry.getValue().idleSince.compareTo(mine);
                  return byTime < 0 || (byTime == 0 && entry.getKey() < shard);
                })
            .count();
  }

  List<SessionView.ShardView> views() {
    List<SessionView.ShardView> views = new ArrayList<>();
    shards.forEach(
        (shard, info) ->
            views.add(
                new SessionView.ShardView(shard, info.departed, info.completed, info.released)));
    return views;
  }

  private ShardInfo infoOf(int shard) {
    return shards.computeIfAbsent(shard, index -> new ShardInfo());
  }

  private static final class ShardInfo {
    boolean departed;
    boolean explicitlyDeparted;
    int completed;
    Instant idleSince;
    boolean released;
    Instant lastSeenAt;
  }
}
