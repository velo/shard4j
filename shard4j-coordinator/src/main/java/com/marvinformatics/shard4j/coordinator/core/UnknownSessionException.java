package com.marvinformatics.shard4j.coordinator.core;

/**
 * Sessions exist only because a registration created one; any other call on an unknown id is
 * answered 404 and never auto-creates. The client recovery is to re-post its registration.
 */
public class UnknownSessionException extends RuntimeException {

  public UnknownSessionException(String sessionId) {
    super("Unknown session: " + sessionId);
  }
}
