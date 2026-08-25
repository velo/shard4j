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
import java.time.Duration;
import java.util.Map;
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
    Path dataDir;
    try {
      dataDir = Files.createTempDirectory(Path.of("target"), "engine-it-data");
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
