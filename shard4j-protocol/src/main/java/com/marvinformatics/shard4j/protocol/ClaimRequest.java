package com.marvinformatics.shard4j.protocol;

import java.util.List;

/** One batched claim per class per pass. Candidates are lease units. */
public record ClaimRequest(int shard, String className, List<String> candidates) {}
