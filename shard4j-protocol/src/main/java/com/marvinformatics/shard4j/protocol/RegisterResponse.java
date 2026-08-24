package com.marvinformatics.shard4j.protocol;

/**
 * @param registeredCount counts lease units, so it is neither a test-method count nor an
 *     invocation count; the verdict prints it every run so a changed value is noticed
 */
public record RegisterResponse(long epoch, int registeredCount) {}
