package com.marvinformatics.shard4j.protocol;

import java.time.Instant;

public record Grant(String testId, Fence fence, Instant expiresAt) {}
