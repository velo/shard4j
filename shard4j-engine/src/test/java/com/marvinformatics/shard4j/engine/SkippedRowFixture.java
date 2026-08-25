package com.marvinformatics.shard4j.engine;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * One template unit whose middle row a per-invocation condition disables: the rows that
 * run pass, so the aggregate must stay PASSED with the skipped row recorded individually.
 */
class SkippedRowFixture {

  @ParameterizedTest
  @ValueSource(strings = {"alpha", "beta", "gamma"})
  @ExtendWith(SkipBeta.class)
  void rows(String value) {}

  static class SkipBeta implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      // The template container's display name carries no argument, so only the one
      // invocation whose argument is "beta" is disabled.
      if (context.getDisplayName().contains("beta")) {
        return ConditionEvaluationResult.disabled("beta is off in this environment");
      }
      return ConditionEvaluationResult.enabled("row enabled");
    }
  }
}
