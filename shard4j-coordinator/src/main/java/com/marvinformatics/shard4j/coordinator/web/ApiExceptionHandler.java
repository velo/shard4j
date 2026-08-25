package com.marvinformatics.shard4j.coordinator.web;

import com.marvinformatics.shard4j.coordinator.core.ProtocolViolationException;
import com.marvinformatics.shard4j.coordinator.core.RegistrationMismatchException;
import com.marvinformatics.shard4j.coordinator.core.StaleFenceException;
import com.marvinformatics.shard4j.coordinator.core.UnknownSessionException;
import com.marvinformatics.shard4j.coordinator.core.UnregisteredTestException;
import com.marvinformatics.shard4j.protocol.ResultResponse;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Status-code mapping for the contract's three interesting answers: 404 never auto-creates,
 * 400 names what was malformed, and a 409 on a result carries the fence that beat the
 * writer so the shard's own log says why.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

  @ExceptionHandler(UnknownSessionException.class)
  public ResponseEntity<Map<String, String>> unknownSession(UnknownSessionException e) {
    return error(HttpStatus.NOT_FOUND, e);
  }

  @ExceptionHandler(ProtocolViolationException.class)
  public ResponseEntity<Map<String, String>> protocolViolation(ProtocolViolationException e) {
    return error(HttpStatus.BAD_REQUEST, e);
  }

  @ExceptionHandler(RegistrationMismatchException.class)
  public ResponseEntity<Map<String, String>> registrationMismatch(RegistrationMismatchException e) {
    return error(HttpStatus.CONFLICT, e);
  }

  @ExceptionHandler(UnregisteredTestException.class)
  public ResponseEntity<Map<String, String>> unregisteredTest(UnregisteredTestException e) {
    return error(HttpStatus.CONFLICT, e);
  }

  @ExceptionHandler(StaleFenceException.class)
  public ResponseEntity<ResultResponse> staleFence(StaleFenceException e) {
    return ResponseEntity.status(HttpStatus.CONFLICT)
        .body(new ResultResponse(false, e.currentFence()));
  }

  private static ResponseEntity<Map<String, String>> error(HttpStatus status, Exception e) {
    return ResponseEntity.status(status).body(Map.of("error", e.getMessage()));
  }
}
