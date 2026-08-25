package com.marvinformatics.shard4j.coordinator.core;

/**
 * Every shard enumerates the same commit, so a census hash disagreeing with the stored one
 * means discovery is non-deterministic. That must scream, not be absorbed by
 * first-writer-wins: the session is registration-mismatched and can never pass.
 */
public class RegistrationMismatchException extends RuntimeException {

  public RegistrationMismatchException(String storedHash, String offeredHash) {
    super(
        "Census hash mismatch: stored "
            + storedHash
            + ", offered "
            + offeredHash
            + "; discovery is non-deterministic across shards and this session can never pass.");
  }
}
