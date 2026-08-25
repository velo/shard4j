package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.SessionVerdict;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.util.Set;
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

  private final Set<TestState> TERMINAL_NON_FAILING =
      Set.of(TestState.PASSED, TestState.SKIPPED, TestState.ABORTED);

  public SessionVerdict of(SessionView view) {
    if (view.registeredCount() <= 0) {
      return SessionVerdict.FAILED;
    }
    long terminalNonFailing =
        view.tests().stream().filter(test -> TERMINAL_NON_FAILING.contains(test.state())).count();
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
