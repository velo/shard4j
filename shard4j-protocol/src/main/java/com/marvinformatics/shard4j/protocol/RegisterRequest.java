package com.marvinformatics.shard4j.protocol;

import java.util.List;
import java.util.Map;

/**
 * The census. Creates the session on first call, idempotent join afterwards, and it is
 * the only creating call. Every shard posts the full enumerated set, so any surviving
 * shard can bootstrap a partial re-run.
 *
 * @param shard 0-based shard index
 * @param attempt the only interpreted run metadata; higher than stored bumps the epoch
 * @param metadata uninterpreted string map, the single seam through which CI-vendor
 *     vocabulary reaches the wire
 * @param testSetHash sha256 hex of the sorted execution-id list
 * @param tests lease units only, never invocation ids
 */
public record RegisterRequest(
    int shard, int attempt, Map<String, String> metadata, String testSetHash, List<String> tests) {}
