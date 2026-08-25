package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.marvinformatics.shard4j.protocol.BarrierRequest;
import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.DepartResponse;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NackResponse;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.RegisterResponse;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.ResultResponse;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import feign.Feign;
import feign.Headers;
import feign.Param;
import feign.RequestLine;
import feign.Response;
import feign.Util;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.SneakyThrows;
import org.testcontainers.containers.GenericContainer;

/**
 * The wire contract as a shard sees it, ridden over the same Feign stack the shipped
 * engine uses -- so a Feign-specific encoding or error-decoding bug fails these tests
 * instead of surviving unseen behind a parallel JDK client.
 */
final class CoordinatorClient {

  static final ObjectMapper JSON =
      JsonMapper.builder()
          .addModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .build();

  /** Status and body of a rejected call, for the tests that assert on rejections. */
  record RawResponse(int status, String body) {

    @SneakyThrows
    <T> T bodyAs(Class<T> type) {
      return JSON.readValue(body, type);
    }
  }

  @Headers("Content-Type: application/json")
  interface Api {

    @RequestLine("POST /sessions/{sessionId}/register")
    RegisterResponse register(@Param("sessionId") String sessionId, RegisterRequest request);

    @RequestLine("POST /sessions/{sessionId}/register")
    Response registerRaw(@Param("sessionId") String sessionId, RegisterRequest request);

    @RequestLine("POST /sessions/{sessionId}/claims")
    ClaimResponse claim(@Param("sessionId") String sessionId, ClaimRequest request);

    @RequestLine("POST /sessions/{sessionId}/claims")
    Response claimRaw(@Param("sessionId") String sessionId, ClaimRequest request);

    @RequestLine("POST /sessions/{sessionId}/results")
    ResultResponse result(@Param("sessionId") String sessionId, ResultRequest request);

    @RequestLine("POST /sessions/{sessionId}/results")
    Response resultRaw(@Param("sessionId") String sessionId, ResultRequest request);

    @RequestLine("POST /sessions/{sessionId}/nack")
    NackResponse nack(@Param("sessionId") String sessionId, NackRequest request);

    @RequestLine("POST /sessions/{sessionId}/nack")
    Response nackRaw(@Param("sessionId") String sessionId, NackRequest request);

    @RequestLine("POST /sessions/{sessionId}/barrier")
    BarrierResponse barrier(@Param("sessionId") String sessionId, BarrierRequest request);

    @RequestLine("POST /sessions/{sessionId}/barrier")
    Response barrierRaw(@Param("sessionId") String sessionId, Object request);

    @RequestLine("POST /sessions/{sessionId}/depart")
    DepartResponse depart(@Param("sessionId") String sessionId, DepartRequest request);

    @RequestLine("POST /sessions/{sessionId}/depart")
    Response departRaw(@Param("sessionId") String sessionId, Object request);

    @RequestLine("GET /sessions/{sessionId}")
    SessionView view(@Param("sessionId") String sessionId);

    @RequestLine("GET /sessions/{sessionId}")
    Response viewRaw(@Param("sessionId") String sessionId);

    @RequestLine("GET /{probe}")
    Response probe(@Param("probe") String probe);
  }

  private final Api api;

  CoordinatorClient(GenericContainer<?> container) {
    this(container, CoordinatorContainers.SECRET);
  }

  CoordinatorClient(GenericContainer<?> container, String secret) {
    Feign.Builder builder =
        Feign.builder().encoder(new JacksonEncoder(JSON)).decoder(new JacksonDecoder(JSON));
    if (secret != null) {
      builder.requestInterceptor(
          template -> template.header("Authorization", "Bearer " + secret));
    }
    this.api =
        builder.target(
            Api.class, "http://" + container.getHost() + ":" + container.getMappedPort(8080));
  }

  RegisterResponse register(String sessionId, RegisterRequest request) {
    return api.register(sessionId, request);
  }

  RawResponse registerRaw(String sessionId, RegisterRequest request) {
    return raw(api.registerRaw(sessionId, request));
  }

  ClaimResponse claim(String sessionId, ClaimRequest request) {
    return api.claim(sessionId, request);
  }

  RawResponse claimRaw(String sessionId, ClaimRequest request) {
    return raw(api.claimRaw(sessionId, request));
  }

  ResultResponse result(String sessionId, ResultRequest request) {
    return api.result(sessionId, request);
  }

  RawResponse resultRaw(String sessionId, ResultRequest request) {
    return raw(api.resultRaw(sessionId, request));
  }

  NackResponse nack(String sessionId, NackRequest request) {
    return api.nack(sessionId, request);
  }

  RawResponse nackRaw(String sessionId, NackRequest request) {
    return raw(api.nackRaw(sessionId, request));
  }

  BarrierResponse barrier(String sessionId, BarrierRequest request) {
    return api.barrier(sessionId, request);
  }

  RawResponse barrierRaw(String sessionId, Object request) {
    return raw(api.barrierRaw(sessionId, request));
  }

  DepartResponse depart(String sessionId, DepartRequest request) {
    return api.depart(sessionId, request);
  }

  RawResponse departRaw(String sessionId, Object request) {
    return raw(api.departRaw(sessionId, request));
  }

  SessionView view(String sessionId) {
    return api.view(sessionId);
  }

  RawResponse viewRaw(String sessionId) {
    return raw(api.viewRaw(sessionId));
  }

  RawResponse probe(String probe) {
    return raw(api.probe(probe));
  }

  TestState stateOf(String sessionId, String testId) {
    return stateOf(view(sessionId), testId);
  }

  static TestState stateOf(SessionView view, String testId) {
    return view.tests().stream()
        .filter(test -> test.testId().equals(testId))
        .findFirst()
        .orElseThrow()
        .state();
  }

  /** Claims exactly one MAIN-pass lease for the unit and returns its fence. */
  Fence claimOne(String sessionId, int shard, String testId) {
    ClaimResponse response =
        claim(
            sessionId,
            new ClaimRequest(shard, Pass.MAIN, Ids.classNameOf(testId), List.of(testId)));
    assertThat(response.granted()).hasSize(1);
    return response.granted().get(0).fence();
  }

  private static RawResponse raw(Response response) {
    try (response) {
      String body =
          response.body() == null
              ? ""
              : Util.toString(response.body().asReader(StandardCharsets.UTF_8));
      return new RawResponse(response.status(), body);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
