package com.marvinformatics.shard4j.coordinator.core;

import java.util.List;

/**
 * Every shard enumerates the same commit, so a census disagreeing with the stored one
 * means discovery is non-deterministic. That must scream, not be absorbed by
 * first-writer-wins: the session is registration-mismatched and can never pass. Because
 * registration carries the whole census, the error names exactly which ids diverged.
 */
public class RegistrationMismatchException extends RuntimeException {

  private static final int NAMED_IDS_CAP = 25;

  public RegistrationMismatchException(List<String> onlyStored, List<String> onlyOffered) {
    super(
        "Census mismatch: discovery is non-deterministic across shards and this session can"
            + " never pass."
            + describe(" Registered by an earlier shard but absent from this one", onlyStored)
            + describe(" Offered by this shard but never registered", onlyOffered));
  }

  private static String describe(String heading, List<String> ids) {
    if (ids.isEmpty()) {
      return "";
    }
    StringBuilder text = new StringBuilder(heading).append(" (").append(ids.size()).append("):");
    ids.stream().limit(NAMED_IDS_CAP).forEach(id -> text.append(' ').append(id));
    if (ids.size() > NAMED_IDS_CAP) {
      text.append(" ... and ").append(ids.size() - NAMED_IDS_CAP).append(" more");
    }
    return text.toString();
  }
}
