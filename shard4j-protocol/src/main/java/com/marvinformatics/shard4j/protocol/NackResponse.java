package com.marvinformatics.shard4j.protocol;

import java.util.List;

/**
 * Per-lease answer to a NACK. Fencing gates a NACK exactly as it gates a result write, and
 * a batch can mix valid and stale leases, so the answer is per entry rather than per call.
 */
public record NackResponse(List<String> released, List<String> rejected) {}
