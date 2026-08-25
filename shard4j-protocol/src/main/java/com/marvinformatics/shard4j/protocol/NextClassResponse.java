package com.marvinformatics.shard4j.protocol;

import java.util.List;

/**
 * The coordinator's choice: the class this shard should drain next -- picked by its
 * schedule, so the class holding the highest-ranked claimable unit -- with that class's
 * first batch of leases granted in the same breath. Granting atomically is what makes a
 * named class never an empty promise: the shard only ever pays a class's setup with
 * leases already in hand. A null {@code className} with no grants means nothing is
 * claimable for this shard right now; further claims for the named class go through the
 * per-class claim endpoint until it is drained.
 */
public record NextClassResponse(String className, List<Grant> granted) {}
