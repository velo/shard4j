package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.Pass;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.junit.platform.engine.ConfigurationParameters;

/**
 * The consumer-facing configuration surface: JUnit configuration parameters (which the
 * launcher already backs with system properties) first, then environment variables
 * ({@code shard.foo.bar} maps to {@code SHARD_FOO_BAR}), with one exception.
 *
 * <p>Absent or false {@code enabled} means the engine is completely inert: no network
 * call, nothing claimed, an empty discovery. With {@code enabled} true, any missing
 * required key is a hard failure naming the key -- never a silent fall-through to running
 * everything.
 *
 * <p>There is no default for {@code coordinatorUrl}, and there never will be.
 *
 * @param coordinatorSecret read from the environment only. A value supplied as a system
 *     property or configuration parameter is refused with an explanation: properties
 *     appear in {@code ps} output, in failsafe's argLine echo, and in crash dumps.
 * @param metadata forwarded verbatim as the registration metadata map
 * @param deadline absolute job-kill time; absent means no early self-release
 * @param concurrency how many classes this shard drains at once; 1 keeps today's strictly
 *     serial behaviour, and anything higher requires the consumer's classes to tolerate
 *     running concurrently in one JVM
 */
public record ShardConfiguration(
    boolean enabled,
    String coordinatorUrl,
    String coordinatorSecret,
    String sessionId,
    int shardIndex,
    Pass pass,
    int attempt,
    int concurrency,
    Map<String, String> metadata,
    Duration retryBudget,
    Instant deadline,
    boolean allLeasedAbortedIsFailure) {

  static final String ENABLED = "shard.enabled";
  static final String COORDINATOR_URL = "shard.coordinator.url";
  static final String SECRET_PROPERTY = "shard.coordinator.secret";
  static final String SECRET_ENVIRONMENT = "SHARD_COORDINATOR_SECRET";
  static final String SESSION_ID = "shard.session.id";
  static final String SHARD_INDEX = "shard.index";
  static final String PASS = "shard.pass";
  static final String ATTEMPT = "shard.attempt";
  static final String CONCURRENCY = "shard.concurrency";
  static final String METADATA_PREFIX = "shard.metadata.";
  static final String RETRY_BUDGET = "shard.coordinator.retry.budget";
  static final String DEADLINE = "shard.deadline";
  static final String ABORT_GUARD = "shard.abort.all-leased-is-failure";

  private static final Duration DEFAULT_RETRY_BUDGET = Duration.ofMinutes(5);

  public static ShardConfiguration resolve(ConfigurationParameters parameters) {
    return resolve(parameters, System.getenv());
  }

  static ShardConfiguration resolve(
      ConfigurationParameters parameters, Map<String, String> environment) {
    Resolver resolver = new Resolver(parameters, environment);
    if (!resolver.flag(ENABLED, false)) {
      return new ShardConfiguration(
          false, null, null, null, -1, null, 1, 1, Map.of(), DEFAULT_RETRY_BUDGET, null, true);
    }
    return new ShardConfiguration(
        true,
        resolver.required(COORDINATOR_URL),
        resolver.secret(),
        resolver.required(SESSION_ID),
        resolver.requiredInt(SHARD_INDEX),
        resolver.pass(),
        resolver.attempt(),
        resolver.concurrency(),
        resolver.metadata(),
        resolver.duration(RETRY_BUDGET, DEFAULT_RETRY_BUDGET),
        resolver.deadline(),
        resolver.flag(ABORT_GUARD, true));
  }

  private record Resolver(ConfigurationParameters parameters, Map<String, String> environment) {

    private String value(String key) {
      return parameters
          .get(key)
          .orElseGet(() -> environment.get(key.replace('.', '_').replace('-', '_').toUpperCase(Locale.ROOT)));
    }

    private String required(String key) {
      String value = value(key);
      if (value == null || value.isBlank()) {
        throw new ShardConfigurationException(
            ENABLED
                + " is true but "
                + key
                + " is not set. Coordinated execution never falls through to running"
                + " everything: set the key or drop "
                + ENABLED
                + ".");
      }
      return value;
    }

    private int requiredInt(String key) {
      String value = required(key);
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        throw new ShardConfigurationException(key + " must be an integer, got: " + value);
      }
    }

    private boolean flag(String key, boolean absentMeans) {
      String value = value(key);
      return value == null ? absentMeans : Boolean.parseBoolean(value);
    }

    /**
     * The one deliberate asymmetry: the secret arrives through the environment only. A
     * system property or configuration parameter is refused rather than ignored, so the
     * mistake is corrected instead of silently duplicated somewhere greppable.
     */
    private String secret() {
      if (parameters.get(SECRET_PROPERTY).isPresent()) {
        throw new ShardConfigurationException(
            SECRET_PROPERTY
                + " must not be supplied as a system property or configuration parameter:"
                + " properties appear in ps output, in the build tool's argLine echo, and in"
                + " crash dumps. Export "
                + SECRET_ENVIRONMENT
                + " instead.");
      }
      String secret = environment.get(SECRET_ENVIRONMENT);
      if (secret == null || secret.isBlank()) {
        throw new ShardConfigurationException(
            ENABLED + " is true but " + SECRET_ENVIRONMENT + " is not exported.");
      }
      return secret;
    }

    private Pass pass() {
      String value = required(PASS);
      try {
        return Pass.valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new ShardConfigurationException(
            PASS + " must be one of main, retry1, retry2 -- got: " + value);
      }
    }

    private int attempt() {
      String value = value(ATTEMPT);
      if (value == null) {
        return 1;
      }
      try {
        int attempt = Integer.parseInt(value);
        if (attempt < 1) {
          throw new NumberFormatException();
        }
        return attempt;
      } catch (NumberFormatException e) {
        throw new ShardConfigurationException(ATTEMPT + " must be a positive integer, got: " + value);
      }
    }

    private int concurrency() {
      String value = value(CONCURRENCY);
      if (value == null) {
        return 1;
      }
      try {
        int concurrency = Integer.parseInt(value);
        if (concurrency < 1) {
          throw new NumberFormatException();
        }
        return concurrency;
      } catch (NumberFormatException e) {
        throw new ShardConfigurationException(
            CONCURRENCY + " must be a positive integer, got: " + value);
      }
    }

    private Duration duration(String key, Duration absentMeans) {
      String value = value(key);
      if (value == null) {
        return absentMeans;
      }
      try {
        return value.regionMatches(true, 0, "P", 0, 1)
            ? Duration.parse(value)
            : Duration.ofSeconds(Long.parseLong(value));
      } catch (NumberFormatException | DateTimeParseException e) {
        throw new ShardConfigurationException(
            key + " must be seconds or an ISO-8601 duration, got: " + value);
      }
    }

    private Instant deadline() {
      String value = value(DEADLINE);
      if (value == null) {
        return null;
      }
      try {
        return Instant.parse(value);
      } catch (DateTimeParseException e) {
        throw new ShardConfigurationException(
            DEADLINE + " must be an ISO-8601 instant, got: " + value);
      }
    }

    /**
     * The single seam through which CI-vendor vocabulary reaches the wire, uninterpreted.
     * Explicit configuration parameters cannot be enumerated past what the launcher was
     * given, so system properties and the environment are swept directly as well.
     */
    private Map<String, String> metadata() {
      Map<String, String> metadata = new TreeMap<>(Comparator.naturalOrder());
      String environmentPrefix =
          METADATA_PREFIX.replace('.', '_').toUpperCase(Locale.ROOT);
      environment.forEach(
          (key, value) -> {
            if (key.startsWith(environmentPrefix)) {
              metadata.put(
                  key.substring(environmentPrefix.length()).toLowerCase(Locale.ROOT), value);
            }
          });
      Properties systemProperties = System.getProperties();
      for (String key : systemProperties.stringPropertyNames()) {
        if (key.startsWith(METADATA_PREFIX)) {
          metadata.put(key.substring(METADATA_PREFIX.length()), systemProperties.getProperty(key));
        }
      }
      for (String key : parameters.keySet()) {
        if (key.startsWith(METADATA_PREFIX)) {
          parameters
              .get(key)
              .ifPresent(value -> metadata.put(key.substring(METADATA_PREFIX.length()), value));
        }
      }
      return Map.copyOf(metadata);
    }
  }
}
