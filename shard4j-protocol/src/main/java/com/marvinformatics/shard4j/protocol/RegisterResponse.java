package com.marvinformatics.shard4j.protocol;

/**
 * @param registeredCount counts registered census units -- method-level, never invocation
 *     expansion, which varies with duration history; the verdict prints it every run so a
 *     changed value is noticed, and only a change to the suite itself may move it
 */
public record RegisterResponse(long epoch, int registeredCount) {}
