package com.marvinformatics.shard4j.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.marvinformatics.shard4j.protocol.SessionView;
import feign.Feign;
import feign.Param;
import feign.RequestLine;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import lombok.experimental.UtilityClass;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Runs the real coordinator jar -- built earlier in this same reactor -- in a real
 * container, so the engine's client machinery is exercised against exactly what a
 * deployer runs: real HTTP, the real auth filter, the real state machine.
 */
@UtilityClass
class CoordinatorContainer {

  final String SECRET = "local-integration-only";
  final String TENANT_KEY = "example/orders-service";
  final String TENANT_SLUG = "orders-service";

  private final ImageFromDockerfile IMAGE =
      new ImageFromDockerfile()
          .withFileFromPath("app.jar", Path.of(System.getProperty("coordinator.app.jar")))
          .withDockerfileFromBuilder(
              dockerfile ->
                  dockerfile
                      .from("eclipse-temurin:25-jre")
                      .copy("app.jar", "/opt/shard4j/app.jar")
                      .entryPoint("java", "-jar", "/opt/shard4j/app.jar")
                      .build());

  GenericContainer<?> start() {
    return start(Map.of());
  }

  GenericContainer<?> start(Map<String, String> extraEnvironment) {
    return start(extraEnvironment, newDataDir());
  }

  /** Tests that seed history before first boot prepare the data directory themselves. */
  Path newDataDir() {
    try {
      return Files.createTempDirectory(Path.of("target"), "engine-it-data");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  GenericContainer<?> start(Map<String, String> extraEnvironment, Path dataDir) {
    Map<String, String> environment = new HashMap<>();
    environment.put("COORDINATOR_SECRETS", SECRET);
    environment.put("COORDINATOR_TENANT_KEY", TENANT_KEY);
    environment.put("COORDINATOR_TENANT_SLUG", TENANT_SLUG);
    environment.put("COORDINATOR_DATA_DIR", "/data");
    environment.putAll(extraEnvironment);
    GenericContainer<?> container =
        new GenericContainer<>(IMAGE)
            .withExposedPorts(8080)
            .withEnv(environment)
            .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data", BindMode.READ_WRITE)
            .withCreateContainerCmdModifier(cmd -> cmd.withUser(currentUidGid()))
            .waitingFor(Wait.forHttp("/readyz").forPort(8080).forStatusCode(200))
            .withStartupTimeout(Duration.ofSeconds(90));
    container.start();
    return container;
  }

  String urlOf(GenericContainer<?> container) {
    return "http://" + container.getHost() + ":" + container.getMappedPort(8080);
  }

  /** The observability surface, for assertions on what the coordinator recorded. */
  SessionView viewOf(GenericContainer<?> container, String sessionId) {
    ObjectMapper json = JsonMapper.builder().addModule(new JavaTimeModule()).build();
    return Feign.builder()
        .encoder(new JacksonEncoder(json))
        .decoder(new JacksonDecoder(json))
        .requestInterceptor(template -> template.header("Authorization", "Bearer " + SECRET))
        .target(ViewApi.class, urlOf(container))
        .view(sessionId);
  }

  interface ViewApi {

    @RequestLine("GET /sessions/{sessionId}")
    SessionView view(@Param("sessionId") String sessionId);
  }

  /**
   * A second shard against the same session, speaking the engine's own production client
   * rather than a test-local restatement of it: a contract change then breaks these tests
   * at compile time, which is the whole reason that interface has no version field.
   */
  CoordinatorClient shardApiOf(GenericContainer<?> container) {
    ObjectMapper json = JsonMapper.builder().addModule(new JavaTimeModule()).build();
    return Feign.builder()
        .encoder(new JacksonEncoder(json))
        .decoder(new JacksonDecoder(json))
        .requestInterceptor(template -> template.header("Authorization", "Bearer " + SECRET))
        .target(CoordinatorClient.class, urlOf(container));
  }

  /** Seeds a day of duration history into a data directory before its container boots. */
  void seedHistory(Path dataDir, Map<String, Long> durationMsByTestId) {
    try {
      Path historyDir = dataDir.resolve(TENANT_SLUG).resolve("history");
      Files.createDirectories(historyDir);
      StringBuilder lines = new StringBuilder();
      durationMsByTestId.forEach(
          (testId, durationMs) ->
              lines
                  .append("{\"type\":\"COMPLETION\",\"project\":\"")
                  .append(TENANT_KEY)
                  .append("\",\"session\":\"seeded-elsewhere\",\"epoch\":1,\"testId\":\"")
                  .append(testId)
                  .append("\",\"unit\":true,\"shard\":0,\"pass\":\"MAIN\",\"outcome\":\"PASSED\"")
                  .append(",\"durationMs\":")
                  .append(durationMs)
                  .append(",\"firstOnShard\":false,\"ts\":\"2026-08-20T10:00:00Z\"}\n"));
      Files.writeString(
          historyDir.resolve(LocalDate.now(ZoneOffset.UTC) + ".jsonl"), lines.toString());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * One template that ran whole and passed, exactly as the coordinator records it: the
   * unit row plus one row per invocation -- which is what makes the breakdown a complete,
   * distributable plan on the next boot.
   */
  void seedTemplateHistory(
      Path dataDir, String templateId, Map<Integer, Long> durationMsByPosition) {
    try {
      Path historyDir = dataDir.resolve(TENANT_SLUG).resolve("history");
      Files.createDirectories(historyDir);
      long total = durationMsByPosition.values().stream().mapToLong(Long::longValue).sum();
      StringBuilder lines = new StringBuilder(historyLine(templateId, true, total));
      new TreeMap<>(durationMsByPosition)
          .forEach(
              (position, durationMs) ->
                  lines.append(
                      historyLine(
                          templateId + "/[test-template-invocation:#" + position + "]",
                          false,
                          durationMs)));
      Files.writeString(
          historyDir.resolve(LocalDate.now(ZoneOffset.UTC) + ".jsonl"),
          lines.toString(),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private String historyLine(String testId, boolean unit, long durationMs) {
    return "{\"type\":\"COMPLETION\",\"project\":\""
        + TENANT_KEY
        + "\",\"session\":\"seeded-elsewhere\",\"epoch\":1,\"testId\":\""
        + testId
        + "\",\"unit\":"
        + unit
        + ",\"shard\":0,\"pass\":\"MAIN\",\"outcome\":\"PASSED\",\"durationMs\":"
        + durationMs
        + (unit ? ",\"firstOnShard\":false" : "")
        + ",\"ts\":\"2026-08-20T10:00:00Z\"}\n";
  }

  private String currentUidGid() {
    try {
      Path probe = Path.of(".");
      int uid = (Integer) Files.getAttribute(probe, "unix:uid");
      int gid = (Integer) Files.getAttribute(probe, "unix:gid");
      return uid + ":" + gid;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
