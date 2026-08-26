package com.marvinformatics.shard4j.protocol;

import java.util.List;

/**
 * Granted is a subset of the candidates intersected with the claimable pool, and
 * capped. An empty grant means the shard skips the class outright -- no {@code @BeforeAll},
 * no class initialiser.
 */
public record ClaimResponse(List<Grant> granted) {}
