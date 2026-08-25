package com.marvinformatics.shard4j.coordinator.it;

import lombok.experimental.UtilityClass;

/** Builders for realistic wire execution ids, so no test hand-rolls the grammar. */
@UtilityClass
class Ids {

  String method(String className, String methodName) {
    return "[engine:junit-jupiter]/[class:" + className + "]/[method:" + methodName + "()]";
  }

  String template(String className, String methodSignature) {
    return "[engine:junit-jupiter]/[class:"
        + className
        + "]/[test-template:"
        + methodSignature
        + "]";
  }

  String invocation(String templateId, int index) {
    return templateId + "/[test-template-invocation:#" + index + "]";
  }

  String classNameOf(String executionId) {
    int start = executionId.indexOf("[class:") + "[class:".length();
    return executionId.substring(start, executionId.indexOf(']', start));
  }
}
