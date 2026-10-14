package com.marvinformatics.shard4j.protocol;

/** What a FAILED execution threw, whole cause chain included; capped by both sides. */
public record Failure(String exceptionClass, String message, String stackTrace) {}
