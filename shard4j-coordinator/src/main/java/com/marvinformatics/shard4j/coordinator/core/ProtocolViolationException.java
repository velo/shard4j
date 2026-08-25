package com.marvinformatics.shard4j.coordinator.core;

/** A malformed request body: answered 400 with the reason, never absorbed silently. */
public class ProtocolViolationException extends RuntimeException {

  public ProtocolViolationException(String message) {
    super(message);
  }
}
