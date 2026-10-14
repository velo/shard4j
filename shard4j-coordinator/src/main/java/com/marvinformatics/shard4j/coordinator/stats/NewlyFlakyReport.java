package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;
import java.util.List;

public record NewlyFlakyReport(
    Instant from, Instant to, Instant baselineFrom, List<NewlyFlakyTest> tests) {}
