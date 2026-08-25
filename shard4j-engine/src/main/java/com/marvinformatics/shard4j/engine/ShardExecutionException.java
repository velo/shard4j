package com.marvinformatics.shard4j.engine;

/** A shard-level failure: the engine root finishes failed and the fork exits non-zero. */
public class ShardExecutionException extends RuntimeException {

  public ShardExecutionException(String message) {
    super(message);
  }
}
