package com.example.orders;

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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.EngineFilter;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Simulates what failsafe's coordinated profile does -- exclude junit-jupiter, hand the
 * suite to the shard4j engine with per-shard configuration -- through the launcher, in
 * process, against the real coordinator jar running in a real container. Three shards are
 * three launcher sessions; three passes are three executions, exactly as the three
 * failsafe execution blocks would run them.
 */
final class ShardingHarness {

  private ShardingHarness() {}

  static final String SECRET = "local-integration-only";

  private static final ImageFromDockerfile IMAGE =
      new ImageFromDockerfile()
          .withFileFromPath("app.jar", Path.of(System.getProperty("coordinator.app.jar")))
          .withDockerfileFromBuilder(
              dockerfile ->
                  dockerfile
                      .from("eclipse-temurin:25-jre")
                      .copy("app.jar", "/opt/shard4j/app.jar")
                      .entryPoint("java", "-jar", "/opt/shard4j/app.jar")
                      .build());

  static GenericContainer<?> startCoordinator() {
    Path dataDir;
    try {
      dataDir = Files.createTempDirectory(Path.of("target"), "sharding-e2e-data");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    GenericContainer<?> container =
        new GenericContainer<>(IMAGE)
            .withExposedPorts(8080)
            .withEnv(
                Map.of(
                    "COORDINATOR_SECRETS", SECRET,
                    "COORDINATOR_TENANT_KEY", "example/orders-service",
                    "COORDINATOR_TENANT_SLUG", "orders-service",
                    "COORDINATOR_DATA_DIR", "/data"))
            .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data", BindMode.READ_WRITE)
            .withCreateContainerCmdModifier(cmd -> cmd.withUser(currentUidGid()))
            .waitingFor(Wait.forHttp("/readyz").forPort(8080).forStatusCode(200))
            .withStartupTimeout(Duration.ofSeconds(90));
    container.start();
    return container;
  }

  static String urlOf(GenericContainer<?> container) {
    return "http://" + container.getHost() + ":" + container.getMappedPort(8080);
  }

  static SessionView viewOf(GenericContainer<?> container, String sessionId) {
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

  /** What one launcher execution observed, for assertions on loudness and cost. */
  record ShardRun(
      List<String> startedTests,
      List<String> testFailures,
      TestExecutionResult engineResult) {}

  static ShardRun runShard(
      String coordinatorUrl, String sessionId, int shard, String pass, List<Class<?>> classes) {
    LauncherDiscoveryRequestBuilder builder =
        LauncherDiscoveryRequestBuilder.request()
            .filters(EngineFilter.excludeEngines("junit-jupiter"))
            .configurationParameter("shard.enabled", "true")
            .configurationParameter("shard.coordinator.url", coordinatorUrl)
            .configurationParameter("shard.session.id", sessionId)
            .configurationParameter("shard.index", Integer.toString(shard))
            .configurationParameter("shard.pass", pass);
    classes.forEach(type -> builder.selectors(DiscoverySelectors.selectClass(type)));
    LauncherDiscoveryRequest request = builder.build();

    List<String> startedTests = new ArrayList<>();
    List<String> testFailures = new ArrayList<>();
    TestExecutionResult[] engineResult = new TestExecutionResult[1];
    TestExecutionListener listener =
        new TestExecutionListener() {
          @Override
          public void executionStarted(TestIdentifier identifier) {
            if (identifier.isTest()) {
              startedTests.add(identifier.getUniqueId());
            }
          }

          @Override
          public void executionFinished(TestIdentifier identifier, TestExecutionResult result) {
            if (identifier.getParentId().isEmpty()) {
              engineResult[0] = result;
            } else if (identifier.isTest()
                && result.getStatus() == TestExecutionResult.Status.FAILED) {
              testFailures.add(identifier.getUniqueId());
            }
          }
        };
    Launcher launcher = LauncherFactory.create();
    launcher.execute(request, listener);
    return new ShardRun(startedTests, testFailures, engineResult[0]);
  }

  private static String currentUidGid() {
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
