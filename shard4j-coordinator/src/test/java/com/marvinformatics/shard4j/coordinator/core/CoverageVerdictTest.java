package com.marvinformatics.shard4j.coordinator.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.SessionVerdict;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CoverageVerdictTest {

  private static SessionView view(List<SessionView.TestView> tests, List<SessionView.ShardView> shards) {
    return new SessionView(
        "7f3a", 1, 1, Map.of(), tests.size(), "hash", shards, tests, List.of(), List.of());
  }

  private static SessionView.TestView test(String id, TestState state) {
    return new SessionView.TestView(id, state, null, List.of());
  }

  @Test
  void allThreeAbsorbingStatesSatisfyCoverage() {
    SessionView view =
        view(
            List.of(
                test("a", TestState.PASSED),
                test("b", TestState.SKIPPED),
                test("c", TestState.ABORTED)),
            List.of(new SessionView.ShardView(0, false, 3)));
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.PASSED);
  }

  @Test
  void aFailedTestFailsTheSession() {
    SessionView view =
        view(
            List.of(test("a", TestState.PASSED), test("b", TestState.FAILED)),
            List.of(new SessionView.ShardView(0, false, 2)));
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.FAILED);
  }

  @Test
  void anEmptySessionNeverPassesBecauseZeroEqualsZeroWouldGoGreenHavingRunNothing() {
    SessionView view = view(List.of(), List.of());
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.FAILED);
  }

  @Test
  void everyShardDepartedWithWorkStrandedIsIncompleteNotFailed() {
    SessionView view =
        view(
            List.of(test("a", TestState.PASSED), test("b", TestState.PENDING)),
            List.of(
                new SessionView.ShardView(0, true, 1), new SessionView.ShardView(1, true, 0)));
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.INCOMPLETE);
  }

  @Test
  void strandedWorkWithALiveShardIsNotYetIncomplete() {
    SessionView view =
        view(
            List.of(test("a", TestState.PENDING)),
            List.of(new SessionView.ShardView(0, false, 0)));
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.FAILED);
  }
}
