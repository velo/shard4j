package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * The one forbidden thing is concurrent double hand-out. Six shards hammer the claim
 * endpoint at once, from a shared starting gun, and every lease unit must be granted to
 * exactly one of them.
 */
class ConcurrentClaimsIT {

  private static final int SHARDS = 6;
  private static final int CLASSES = 8;
  private static final int METHODS_PER_CLASS = 8;

  static GenericContainer<?> coordinator;

  @BeforeAll
  static void start() throws IOException {
    coordinator =
        CoordinatorContainers.coordinator(
            Files.createTempDirectory(Path.of("target"), "concurrent-claims-data"), Map.of());
    coordinator.start();
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void sixShardsClaimingConcurrentlyReceiveEveryUnitExactlyOnce() throws Exception {
    List<String> classNames = new ArrayList<>();
    List<String> census = new ArrayList<>();
    for (int classIndex = 0; classIndex < CLASSES; classIndex++) {
      String className = "com.example.orders.Burst" + classIndex + "IT";
      classNames.add(className);
      for (int methodIndex = 0; methodIndex < METHODS_PER_CLASS; methodIndex++) {
        census.add(Ids.method(className, "case" + methodIndex));
      }
    }
    String sessionId = UUID.randomUUID().toString();

    Queue<String> allGrantedIds = new ConcurrentLinkedQueue<>();
    CountDownLatch startingGun = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(SHARDS);
    List<Future<?>> shards = new ArrayList<>();
    for (int shard = 0; shard < SHARDS; shard++) {
      int shardIndex = shard;
      shards.add(
          pool.submit(
              () -> {
                CoordinatorClient client = new CoordinatorClient(coordinator);
                client.register(
                    sessionId, new RegisterRequest(shardIndex, 1, Map.of(), census));
                startingGun.await();
                boolean anyGrantedInSweep = true;
                while (anyGrantedInSweep) {
                  anyGrantedInSweep = false;
                  for (String className : classNames) {
                    List<String> candidates =
                        census.stream().filter(id -> id.contains(className)).toList();
                    ClaimResponse response =
                        client.claim(
                            sessionId,
                            new ClaimRequest(shardIndex, Pass.MAIN, className, candidates));
                    if (!response.granted().isEmpty()) {
                      anyGrantedInSweep = true;
                      response.granted().stream().map(Grant::testId).forEach(allGrantedIds::add);
                    }
                  }
                }
                return null;
              }));
    }
    startingGun.countDown();
    for (Future<?> shard : shards) {
      shard.get(120, TimeUnit.SECONDS);
    }
    pool.shutdown();

    List<String> granted = List.copyOf(allGrantedIds);
    Set<String> distinct = new HashSet<>(granted);
    assertThat(granted).as("no unit was handed out twice").hasSameSizeAs(distinct);
    assertThat(distinct).as("no unit was left unhanded").containsExactlyInAnyOrderElementsOf(census);
  }
}
