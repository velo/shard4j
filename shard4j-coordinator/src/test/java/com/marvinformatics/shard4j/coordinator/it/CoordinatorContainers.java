package com.marvinformatics.shard4j.coordinator.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Runs the real repackaged coordinator jar in a real container: the image is built from
 * the module's own build output as part of the test run, so what these tests exercise is
 * exactly what a deployer runs -- boot wiring, the auth filter, real HTTP, a real volume.
 *
 * <p>The container runs as the invoking user so files written into the bind-mounted data
 * directory stay deletable by the build.
 */
@UtilityClass
class CoordinatorContainers {

  final String SECRET = "local-integration-only";
  final String TENANT_KEY = "example/orders-service";
  final String TENANT_SLUG = "orders-service";

  // Auto-named on purpose: a fixed image name is silently reused from a previous build,
  // and tests against a stale jar are worse than a few seconds of rebuild.
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

  GenericContainer<?> coordinator(Path dataDir, Map<String, String> extraEnv) {
    Map<String, String> env = new HashMap<>();
    env.put("COORDINATOR_SECRETS", SECRET);
    env.put("COORDINATOR_TENANT_KEY", TENANT_KEY);
    env.put("COORDINATOR_TENANT_SLUG", TENANT_SLUG);
    env.put("COORDINATOR_DATA_DIR", "/data");
    env.putAll(extraEnv);
    GenericContainer<?> container =
        new GenericContainer<>(IMAGE)
            .withExposedPorts(8080)
            .withEnv(env)
            .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data", BindMode.READ_WRITE)
            .withCreateContainerCmdModifier(cmd -> cmd.withUser(currentUidGid()))
            .waitingFor(Wait.forHttp("/readyz").forPort(8080).forStatusCode(200))
            .withStartupTimeout(Duration.ofSeconds(90));
    return container;
  }

  /** For the refuse-to-start test: no env defaults beyond what is given. */
  GenericContainer<?> bareCoordinator(Path dataDir, Map<String, String> env) {
    return new GenericContainer<>(IMAGE)
        .withExposedPorts(8080)
        .withEnv(env)
        .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data", BindMode.READ_WRITE)
        .withCreateContainerCmdModifier(cmd -> cmd.withUser(currentUidGid()))
        .waitingFor(Wait.forHttp("/readyz").forPort(8080).forStatusCode(200))
        .withStartupTimeout(Duration.ofSeconds(20));
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
