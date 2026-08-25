package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.BarrierRequest;
import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NextClassRequest;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.RegisterResponse;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import feign.Headers;
import feign.Param;
import feign.RequestLine;

/**
 * The shard's half of the HTTP contract.
 *
 * <p>There is no protocol version field on the wire and one must not be added: the
 * coordinator and this client are built, versioned and released from one reactor, so any
 * change to the contract breaks this compilation first, at build time, where it is
 * cheapest to see.
 *
 * <p>The shared secret travels as {@code Authorization: Bearer} on every mutating call,
 * added by a {@code RequestInterceptor} rather than by a parameter, so it cannot end up
 * in a log line or a URL.
 */
@Headers("Content-Type: application/json")
public interface CoordinatorClient {

  @RequestLine("POST /sessions/{sessionId}/register")
  RegisterResponse register(@Param("sessionId") String sessionId, RegisterRequest request);

  @RequestLine("POST /sessions/{sessionId}/claims")
  ClaimResponse claim(@Param("sessionId") String sessionId, ClaimRequest request);

  @RequestLine("POST /sessions/{sessionId}/next")
  NextClassResponse next(@Param("sessionId") String sessionId, NextClassRequest request);

  @RequestLine("POST /sessions/{sessionId}/results")
  void result(@Param("sessionId") String sessionId, ResultRequest request);

  @RequestLine("POST /sessions/{sessionId}/nack")
  void nack(@Param("sessionId") String sessionId, NackRequest request);

  @RequestLine("POST /sessions/{sessionId}/barrier")
  BarrierResponse barrier(@Param("sessionId") String sessionId, BarrierRequest request);

  @RequestLine("POST /sessions/{sessionId}/depart")
  void depart(@Param("sessionId") String sessionId, DepartRequest request);
}
