package com.marvinformatics.shard4j.coordinator.stats;

public record MostFrequentFailure(
    String fingerprint, String exceptionClass, String message, int occurrences) {}
