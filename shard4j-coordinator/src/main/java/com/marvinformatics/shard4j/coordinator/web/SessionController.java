package com.marvinformatics.shard4j.coordinator.web;

import com.marvinformatics.shard4j.coordinator.core.CoordinatorCore;
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
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * The HTTP surface is a thin adapter: every semantic decision lives in the core, where it
 * is testable against the real state machine rather than against framework plumbing.
 */
@RestController
@RequiredArgsConstructor
public class SessionController {

  private final CoordinatorCore core;

  @PostMapping("/sessions/{sessionId}/register")
  public RegisterResponse register(
      @PathVariable String sessionId, @RequestBody RegisterRequest request) {
    return core.register(sessionId, request);
  }

  @PostMapping("/sessions/{sessionId}/claims")
  public ClaimResponse claim(@PathVariable String sessionId, @RequestBody ClaimRequest request) {
    return core.claim(sessionId, request);
  }

  @PostMapping("/sessions/{sessionId}/results")
  public ResultResponse result(@PathVariable String sessionId, @RequestBody ResultRequest request) {
    return core.result(sessionId, request);
  }

  @PostMapping("/sessions/{sessionId}/nack")
  public NackResponse nack(@PathVariable String sessionId, @RequestBody NackRequest request) {
    return core.nack(sessionId, request);
  }

  @PostMapping("/sessions/{sessionId}/depart")
  public DepartResponse depart(@PathVariable String sessionId, @RequestBody DepartRequest request) {
    return core.depart(sessionId, request);
  }

  @GetMapping("/sessions/{sessionId}")
  public SessionView view(@PathVariable String sessionId) {
    return core.view(sessionId);
  }
}
