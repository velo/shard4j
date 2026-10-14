package com.marvinformatics.shard4j.coordinator.stats;

/** The most frequent fault behind a test's failures in the window. */
public record TopFailure(
    String fingerprint, String exceptionClass, String message, int occurrences) {}
