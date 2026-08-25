package com.marvinformatics.shard4j.coordinator.web;

import com.marvinformatics.shard4j.coordinator.storage.DataDirectory;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Both probes are unauthenticated and return no tenant data and no secret material.
 * Readiness implies boot replay completed and the incarnation was fsynced: those happen
 * during wiring, before the listener serves anything, so a served answer is proof.
 */
@RestController
@RequiredArgsConstructor
public class HealthController {

  private final DataDirectory dataDirectory;

  @GetMapping("/healthz")
  public ResponseEntity<Map<String, String>> healthz() {
    if (!dataDirectory.lockHeld()) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(Map.of("status", "data-directory lock lost"));
    }
    return ResponseEntity.ok(Map.of("status", "ok"));
  }

  @GetMapping("/readyz")
  public ResponseEntity<Map<String, String>> readyz() {
    return healthz();
  }
}
