package com.marvinformatics.shard4j.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.experimental.UtilityClass;

/** Shared journal for the ordering fixtures: each probe method appends its own label. */
@UtilityClass
class OrderProbeRecorder {

  final List<String> EVENTS = Collections.synchronizedList(new ArrayList<>());

  void record(String label) {
    EVENTS.add(label);
  }

  void reset() {
    EVENTS.clear();
  }
}
