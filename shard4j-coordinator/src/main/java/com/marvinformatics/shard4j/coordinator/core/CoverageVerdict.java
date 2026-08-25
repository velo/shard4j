package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.SessionVerdict;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import lombok.experimental.UtilityClass;

/**
 * The coverage rule, as pure logic over the read model: the session passed if and only if
 * every registered lease unit reached a terminal non-failing state. Never shard exit codes,
 * never queue emptiness -- a drained queue proves nothing about what actually ran.
 *
 * <p>{@code registeredCount > 0} guards a replay into an empty session, where zero-equals-
 * zero would go green having run nothing.
 */
@UtilityClass
public class CoverageVerdict {

  public SessionVerdict of(SessionView view) {
    if (view.registeredCount() <= 0) {
      return SessionVerdict.FAILED;
    }
    long terminalNonFailing =
        view.tests().stream().filter(test -> test.state().isAbsorbing()).count();
    if (terminalNonFailing == view.registeredCount()) {
      return SessionVerdict.PASSED;
    }
    boolean everyShardDeparted =
        !view.shards().isEmpty() && view.shards().stream().allMatch(SessionView.ShardView::departed);
    boolean nonTerminalRemain =
        view.tests().stream()
            .anyMatch(
                test -> test.state() == TestState.PENDING || test.state() == TestState.LEASED);
    if (everyShardDeparted && nonTerminalRemain) {
      return SessionVerdict.INCOMPLETE;
    }
    return SessionVerdict.FAILED;
  }
}
