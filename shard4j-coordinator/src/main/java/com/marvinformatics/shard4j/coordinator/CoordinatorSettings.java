package com.marvinformatics.shard4j.coordinator;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

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
 *     count of accepted values and nothing else. The variable is split on commas, so a
 *     secret value must not contain one -- it would silently become two wrong values; a
 *     blank entry after the split (the tell-tale of a stray comma) is a refused start.
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
    @DefaultValue("/data") Path dataDir,
    @DefaultValue("false") boolean publicRead,
    @DefaultValue("20m") Duration leaseTtl,
    @DefaultValue("8") int maxClaimBatch,
    @DefaultValue("60m") Duration durationClamp,
    @DefaultValue("7d") Duration gcIdle,
    @DefaultValue("30d") Duration historyRetention) {

  private static final String SLUG_PATTERN = "[A-Za-z0-9._-]{1,64}";

  /**
   * The three required keys are checked here rather than left to fail later: a coordinator
   * with no accepted secret would accept writes from anyone who found the port, so it must
   * refuse to start, naming the variable a deployer has to set.
   */
  public void requireCompleteness() {
    if (secrets == null || secrets.isEmpty() || secrets.stream().allMatch(String::isBlank)) {
      throw new IllegalStateException(
          "Refusing to start: COORDINATOR_SECRETS is absent or empty."
              + " The coordinator never runs with authentication disabled;"
              + " set at least one accepted secret value.");
    }
    if (secrets.stream().anyMatch(String::isBlank)) {
      throw new IllegalStateException(
          "Refusing to start: COORDINATOR_SECRETS contains a blank entry -- usually a stray"
              + " comma. The variable is split on commas, so a secret value must not"
              + " contain one.");
    }
    if (tenantKey == null || tenantKey.isBlank()) {
      throw new IllegalStateException(
          "Refusing to start: COORDINATOR_TENANT_KEY is required and has no default.");
    }
    if (tenantSlug == null || !tenantSlug.matches(SLUG_PATTERN)) {
      throw new IllegalStateException(
          "Refusing to start: COORDINATOR_TENANT_SLUG must match " + SLUG_PATTERN + ".");
    }
  }
}
