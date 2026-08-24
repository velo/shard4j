package com.marvinformatics.shard4j.protocol;

/** The three retry passes, one per failsafe execution block. */
public enum Pass {
  MAIN,
  RETRY1,
  RETRY2
}
