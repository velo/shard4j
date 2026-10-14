package com.marvinformatics.shard4j.coordinator.web;

/** A read-surface query parameter that cannot be understood; answered 400, naming it. */
public final class InvalidQueryException extends RuntimeException {

  public InvalidQueryException(String message) {
    super(message);
  }
}
