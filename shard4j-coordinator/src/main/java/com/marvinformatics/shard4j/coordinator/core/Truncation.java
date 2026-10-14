package com.marvinformatics.shard4j.coordinator.core;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Truncation {

  public String truncate(String text, int maxChars) {
    return text == null || text.length() <= maxChars ? text : text.substring(0, maxChars);
  }
}
