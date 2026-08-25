package com.marvinformatics.shard4j.protocol;

import static org.assertj.core.api.Assertions.assertThat;

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
        new Object[] {
          "POST /sessions/{id}/next", NextClassRequest.class, NextClassResponse.class
        },
        new Object[] {"POST /sessions/{id}/results", ResultRequest.class, ResultResponse.class},
        new Object[] {"POST /sessions/{id}/nack", NackRequest.class, NackResponse.class},
        new Object[] {"POST /sessions/{id}/barrier", BarrierRequest.class, BarrierResponse.class},
        new Object[] {"POST /sessions/{id}/depart", DepartRequest.class, DepartResponse.class});
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("endpoints")
  void carriesARecordOnBothSides(String endpoint, Class<?> request, Class<?> response) {
    assertThat(request.isRecord()).as(endpoint + " request must be a record").isTrue();
    assertThat(response.isRecord()).as(endpoint + " response must be a record").isTrue();
  }

  @Test
  void theReadEndpointIsARecordWithNoRequestBody() {
    assertThat(SessionView.class.isRecord())
        .as("GET /sessions/{id} returns the session view")
        .isTrue();
  }

  @Test
  void aStaleResultIsRejectedWithTheFenceThatBeatIt() {
    ResultResponse rejected = new ResultResponse(false, new Fence(2, 5, 7));

    assertThat(rejected.accepted()).isFalse();
    assertThat(rejected.currentFence()).isEqualTo(new Fence(2, 5, 7));
  }

  @Test
  void aNackAnswersPerLeaseBecauseStaleEntriesAreRejectedIndividually() {
    NackResponse response = new NackResponse(List.of("released-id"), List.of("stale-id"));

    assertThat(response.released()).containsExactlyElementsOf(List.of("released-id"));
    assertThat(response.rejected()).containsExactlyElementsOf(List.of("stale-id"));
  }

  @Test
  void aRegistrationMismatchNamesBothHashes() {
    RegisterResponse response = new RegisterResponse(1, 61);

    assertThat(response.epoch()).isOne();
    assertThat(response.registeredCount()).isEqualTo(61);
    assertThat(new RegisterRequest(3, 2, Map.of("ci", "github-actions"), List.of()).metadata())
        .containsExactlyInAnyOrderEntriesOf(Map.of("ci", "github-actions"));
  }
}
