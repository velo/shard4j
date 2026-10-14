package com.marvinformatics.shard4j.coordinator.stats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FailureFingerprintTest {

  private static final String TIMEOUT = "java.net.SocketTimeoutException";

  @Test
  void givenTheSameFaultOnDifferentLinesAndLambdas_whenFingerprinting_thenTheyMatch() {
    String before =
        """
        java.net.SocketTimeoutException: Read timed out after 30001 ms
        \tat com.example.orders.GatewayClient.call(GatewayClient.java:88)
        \tat com.example.orders.CheckoutIT$$Lambda/0x00007f3c4c0a1234.run(Unknown Source)
        \tat com.example.orders.CheckoutIT$$Lambda$14/0x0000000800c03000.run(Unknown Source)
        """;
    String after =
        """
        java.net.SocketTimeoutException: Read timed out after 30017 ms
        \tat com.example.orders.GatewayClient.call(GatewayClient.java:91)
        \tat com.example.orders.CheckoutIT$$Lambda/0x00007f3c4c0b9876.run(Unknown Source)
        \tat com.example.orders.CheckoutIT$$Lambda$27/0x0000000800d14000.run(Unknown Source)
        """;

    assertThat(FailureFingerprint.of(TIMEOUT, before)).isEqualTo(FailureFingerprint.of(TIMEOUT, after));
  }

  @Test
  void givenTheSameTopFrameButADifferentRootCause_whenFingerprinting_thenTheyDiffer() {
    String trace =
        """
        java.lang.IllegalStateException: checkout failed
        \tat com.example.orders.Checkout.run(Checkout.java:10)
        Caused by: %s: boom
        \tat com.example.orders.Gateway.call(Gateway.java:20)
        """;

    assertThat(FailureFingerprint.of("java.lang.IllegalStateException", trace.formatted(TIMEOUT)))
        .isNotEqualTo(
            FailureFingerprint.of(
                "java.lang.IllegalStateException", trace.formatted("java.net.ConnectException")));
  }

  @Test
  void givenADifferentCallSite_whenFingerprinting_thenTheyDiffer() {
    assertThat(FailureFingerprint.of(TIMEOUT, "\tat com.example.orders.A.call(A.java:1)"))
        .isNotEqualTo(FailureFingerprint.of(TIMEOUT, "\tat com.example.orders.B.call(B.java:1)"));
  }
}
