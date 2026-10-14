package com.marvinformatics.shard4j.coordinator.stats;

import java.util.List;
import java.util.Map;

public record FailureReport(
    List<FailedAttempt> failedAttempts, Map<String, DistinctFailure> distinctFailures) {}
