package com.marvinformatics.shard4j.protocol;

import java.util.List;

/** One batched claim per class per pass. Candidates are lease units. */
public record ClaimRequest(int shard, Pass pass, String className, List<String> candidates) {}
