package com.marvinformatics.shard4j.coordinator;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The whole configuration surface, bound from environment variables because those are the
 * lowest common denominator across orchestrators, systemd and a laptop.
 *
 * <p>No key's value names a host, cluster, cloud or organisation. The three required keys
 * have no defaults on purpose: there is no sensible default secret and no sensible default
 * tenant, and a service that starts with an implicit one starts unauthenticated.
 *
 * @param secrets {@code COORDINATOR_SECRETS}. A set, not a value, so rotation is two
 *     independent deploys with an overlap window. The process must refuse to start when it
 *     is absent or empty, naming the variable. Compared in constant time, never logged,
 *     never echoed -- not in an error body, not at DEBUG, not in a startup banner. Log the
 *     count of accepted values and nothing else.
 * @param tenantKey {@code COORDINATOR_TENANT_KEY}. Opaque: not parsed, not assumed to be
 *     owner/name, not assumed to be Git. The tenant is a property of this instance's
 *     configuration and is never client-supplied; the wire has no tenant field at all.
 * @param tenantSlug {@code COORDINATOR_TENANT_SLUG}, matching {@code [A-Za-z0-9._-]{1,64}},
 *     so no character of a tenant key can affect the on-disk layout.
 * @param dataDir the one writable directory, locked exclusively at startup
 * @param publicRead serve the read surface without a secret. Off by default: a session id
 *     is not a capability, and for a public repository it appears in public logs.
 * @param leaseTtl must comfortably exceed the consumer's slowest test
 * @param durationClamp set to the consumer's shard job timeout
 * @param gcIdle bounds how long a partial re-run can rejoin; pick generously
 */
@ConfigurationProperties(prefix = "coordinator")
public record CoordinatorSettings(
    Set<String> secrets,
    String tenantKey,
    String tenantSlug,
    Path dataDir,
    boolean publicRead,
    Duration leaseTtl,
    int maxClaimBatch,
    Duration durationClamp,
    Duration gcIdle,
    Duration historyRetention) {}
