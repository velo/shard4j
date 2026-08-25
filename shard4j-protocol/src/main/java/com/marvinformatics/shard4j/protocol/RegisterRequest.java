package com.marvinformatics.shard4j.protocol;

import java.util.List;
import java.util.Map;

/**
 * The census. Creates the session on first call, idempotent join afterwards, and it is
 * the only creating call. Every shard posts the full enumerated set, so any surviving
 * shard can bootstrap a partial re-run -- and so the coordinator can compare censuses as
 * sets and name exactly which ids diverged, rather than comparing digests whose encoding
 * (separator, collation, charset) both sides would have to agree on forever.
 *
 * @param shard 0-based shard index
 * @param attempt the only interpreted run metadata; higher than stored bumps the epoch
 * @param metadata uninterpreted string map, the single seam through which CI-vendor
 *     vocabulary reaches the wire
 * @param tests lease units only, never invocation ids
 * @param shardCount how many shards this run launched, when the consumer knows it; null
 *     when it does not. Purely a balancing hint: it lets the coordinator hold back a fair
 *     share of a template's invocations for shards that have not registered yet, instead
 *     of granting them all to whichever shard asked first. Never part of any quorum.
 */
public record RegisterRequest(
    int shard, int attempt, Map<String, String> metadata, List<String> tests, Integer shardCount) {}
