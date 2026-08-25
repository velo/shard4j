package com.marvinformatics.shard4j.engine;

/**
 * A misconfigured shard fails the fork loudly, naming the key. The alternative -- falling
 * through to running everything -- is the silent double-execution this engine exists to
 * make impossible.
 */
public class ShardConfigurationException extends RuntimeException {

  public ShardConfigurationException(String message) {
    super(message);
  }
}
