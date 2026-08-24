package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.DepartResponse;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NackResponse;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.RegisterResponse;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.ResultResponse;
import com.marvinformatics.shard4j.protocol.SessionView;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import org.testcontainers.containers.GenericContainer;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * A deliberately thin HTTP client over the wire records: real requests, real JSON, no
 * framework on the test side, so what these tests prove is the contract as a shard sees it.
 */
final class CoordinatorClient {

  static final ObjectMapper JSON = JsonMapper.builder().build();

  private final HttpClient http = HttpClient.newHttpClient();
  private final String base;
  private final String secret;

  CoordinatorClient(GenericContainer<?> container) {
    this(container, CoordinatorContainers.SECRET);
  }

  CoordinatorClient(GenericContainer<?> container, String secret) {
    this.base = "http://" + container.getHost() + ":" + container.getMappedPort(8080);
    this.secret = secret;
  }

  HttpResponse<String> post(String path, Object body) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(base + path))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
    if (secret != null) {
      request.header("Authorization", "Bearer " + secret);
    }
    return send(request.build());
  }

  HttpResponse<String> get(String path) {
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path)).GET();
    if (secret != null) {
      request.header("Authorization", "Bearer " + secret);
    }
    return send(request.build());
  }

  RegisterResponse register(String sessionId, RegisterRequest request) {
    HttpResponse<String> response = post("/sessions/" + sessionId + "/register", request);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return JSON.readValue(response.body(), RegisterResponse.class);
  }

  ClaimResponse claim(String sessionId, ClaimRequest request) {
    HttpResponse<String> response = post("/sessions/" + sessionId + "/claims", request);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return JSON.readValue(response.body(), ClaimResponse.class);
  }

  ResultResponse result(String sessionId, ResultRequest request) {
    HttpResponse<String> response = resultRaw(sessionId, request);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return JSON.readValue(response.body(), ResultResponse.class);
  }

  HttpResponse<String> resultRaw(String sessionId, ResultRequest request) {
    return post("/sessions/" + sessionId + "/results", request);
  }

  NackResponse nack(String sessionId, NackRequest request) {
    HttpResponse<String> response = post("/sessions/" + sessionId + "/nack", request);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return JSON.readValue(response.body(), NackResponse.class);
  }

  DepartResponse depart(String sessionId, DepartRequest request) {
    HttpResponse<String> response = post("/sessions/" + sessionId + "/depart", request);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return JSON.readValue(response.body(), DepartResponse.class);
  }

  SessionView view(String sessionId) {
    HttpResponse<String> response = get("/sessions/" + sessionId);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return JSON.readValue(response.body(), SessionView.class);
  }

  static String hashOf(List<String> testIds) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      String canonical = String.join("\n", testIds.stream().sorted().toList());
      return HexFormat.of().formatHex(digest.digest(canonical.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private HttpResponse<String> send(HttpRequest request) {
    try {
      return http.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
