package com.marvinformatics.shard4j.coordinator.core;

/** A test id outside the registered census is a conflict, never auto-registered. */
public class UnregisteredTestException extends RuntimeException {

  public UnregisteredTestException(String testId) {
    super("Not in the registered census: " + testId);
  }
}
