package com.marvinformatics.shard4j.coordinator.it;

/** Builders for realistic wire execution ids, so no test hand-rolls the grammar. */
final class Ids {

  private Ids() {}

  static String method(String className, String methodName) {
    return "[engine:junit-jupiter]/[class:" + className + "]/[method:" + methodName + "()]";
  }

  static String template(String className, String methodSignature) {
    return "[engine:junit-jupiter]/[class:"
        + className
        + "]/[test-template:"
        + methodSignature
        + "]";
  }

  static String invocation(String templateId, int index) {
    return templateId + "/[test-template-invocation:#" + index + "]";
  }

  static String classNameOf(String executionId) {
    int start = executionId.indexOf("[class:") + "[class:".length();
    return executionId.substring(start, executionId.indexOf(']', start));
  }
}
