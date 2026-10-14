package com.marvinformatics.shard4j.coordinator.web;

/** A read-surface query parameter that cannot be understood; answered 400, naming it. */
public final class InvalidQueryParameterException extends RuntimeException {

  public InvalidQueryParameterException(String message) {
    super(message);
  }
}
