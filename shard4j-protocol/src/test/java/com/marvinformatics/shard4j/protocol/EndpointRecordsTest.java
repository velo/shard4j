package com.marvinformatics.shard4j.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Every endpoint of the HTTP contract has a body type on each side that carries a body. */
class EndpointRecordsTest {

  static Stream<Object[]> endpoints() {
    return Stream.of(
        new Object[] {
          "POST /sessions/{id}/register", RegisterRequest.class, RegisterResponse.class
        },
        new Object[] {"POST /sessions/{id}/claims", ClaimRequest.class, ClaimResponse.class},
        new Object[] {"POST /sessions/{id}/results", ResultRequest.class, ResultResponse.class},
        new Object[] {"POST /sessions/{id}/nack", NackRequest.class, NackResponse.class},
        new Object[] {"POST /sessions/{id}/barrier", BarrierRequest.class, BarrierResponse.class},
        new Object[] {"POST /sessions/{id}/depart", DepartRequest.class, DepartResponse.class});
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("endpoints")
  void carriesARecordOnBothSides(String endpoint, Class<?> request, Class<?> response) {
    assertTrue(request.isRecord(), endpoint + " request must be a record");
    assertTrue(response.isRecord(), endpoint + " response must be a record");
  }

  @Test
  void theReadEndpointIsARecordWithNoRequestBody() {
    assertTrue(SessionView.class.isRecord(), "GET /sessions/{id} returns the session view");
  }

  @Test
  void aStaleResultIsRejectedWithTheFenceThatBeatIt() {
    ResultResponse rejected = new ResultResponse(false, new Fence(2, 5, 7));

    assertEquals(false, rejected.accepted());
    assertEquals(new Fence(2, 5, 7), rejected.currentFence());
  }

  @Test
  void aNackAnswersPerLeaseBecauseStaleEntriesAreRejectedIndividually() {
    NackResponse response = new NackResponse(List.of("released-id"), List.of("stale-id"));

    assertEquals(List.of("released-id"), response.released());
    assertEquals(List.of("stale-id"), response.rejected());
  }

  @Test
  void aRegistrationMismatchNamesBothHashes() {
    RegisterResponse response = new RegisterResponse(1, 61);

    assertEquals(1, response.epoch());
    assertEquals(61, response.registeredCount());
    assertEquals(
        Map.of("ci", "github-actions"),
        new RegisterRequest(3, 2, Map.of("ci", "github-actions"), List.of()).metadata());
  }
}
