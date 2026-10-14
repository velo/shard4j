package com.marvinformatics.shard4j.coordinator.stats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FailureFingerprintTest {

  private static final String TIMEOUT_EXCEPTION_CLASS = "java.net.SocketTimeoutException";

  @Test
  void
      givenTwoTimeoutTracesDifferingOnlyInMessageLineNumbersAndLambdaClassNames_whenFingerprinting_thenFingerprintsAreEqual() {
    String firstRunTrace =
        """
        java.net.SocketTimeoutException: Read timed out after 30001 ms
        \tat com.example.orders.GatewayClient.call(GatewayClient.java:88)
        \tat com.example.orders.CheckoutIT$$Lambda/0x00007f3c4c0a1234.run(Unknown Source)
        \tat com.example.orders.CheckoutIT$$Lambda$14/0x0000000800c03000.run(Unknown Source)
        """;
    String secondRunTrace =
        """
        java.net.SocketTimeoutException: Read timed out after 30017 ms
        \tat com.example.orders.GatewayClient.call(GatewayClient.java:91)
        \tat com.example.orders.CheckoutIT$$Lambda/0x00007f3c4c0b9876.run(Unknown Source)
        \tat com.example.orders.CheckoutIT$$Lambda$27/0x0000000800d14000.run(Unknown Source)
        """;

    assertThat(FailureFingerprint.hashNormalizedTrace(TIMEOUT_EXCEPTION_CLASS, firstRunTrace))
        .isEqualTo(FailureFingerprint.hashNormalizedTrace(TIMEOUT_EXCEPTION_CLASS, secondRunTrace));
  }

  @Test
  void givenTracesDifferingOnlyInRootCauseType_whenFingerprinting_thenTheFingerprintsDiffer() {
    String traceCausedBy =
        """
        java.lang.IllegalStateException: checkout failed
        \tat com.example.orders.Checkout.run(Checkout.java:10)
        Caused by: %s: boom
        \tat com.example.orders.Gateway.call(Gateway.java:20)
        """;

    assertThat(
            FailureFingerprint.hashNormalizedTrace(
                "java.lang.IllegalStateException",
                traceCausedBy.formatted(TIMEOUT_EXCEPTION_CLASS)))
        .isNotEqualTo(
            FailureFingerprint.hashNormalizedTrace(
                "java.lang.IllegalStateException",
                traceCausedBy.formatted("java.net.ConnectException")));
  }

  @Test
  void
      givenTheSameExceptionThrownFromADifferentClass_whenFingerprinting_thenTheFingerprintsDiffer() {
    assertThat(
            FailureFingerprint.hashNormalizedTrace(
                TIMEOUT_EXCEPTION_CLASS, "\tat com.example.orders.A.call(A.java:1)"))
        .isNotEqualTo(
            FailureFingerprint.hashNormalizedTrace(
                TIMEOUT_EXCEPTION_CLASS, "\tat com.example.orders.B.call(B.java:1)"));
  }
}
