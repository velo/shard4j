package com.marvinformatics.shard4j.protocol;

/**
 * A unique id that does not match the execution-id grammar. It always names the offending
 * id: a stale or unsupported id is silently dropped by JUnit itself, so this is the last
 * place it can be reported at all.
 */
public class MalformedExecutionIdException extends IllegalArgumentException {

  private static final long serialVersionUID = 1L;

  private final String id;

  public MalformedExecutionIdException(String id, String reason) {
    super("Not a supported execution id: " + id + " -- " + reason);
    this.id = id;
  }

  public String id() {
    return id;
  }
}
