package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

class CompatibilityMatrixTest {

  private static final CompatibilityMatrix.Entry ENTRY =
      new CompatibilityMatrix.Entry(
          "surefire/failsafe", "maven-artifact", "org.apache.maven.surefire:surefire-booter",
          null, "3.6.0", "3.6.0");

  @Test
  void givenTheShippedCatalog_whenReadingIt_thenEveryEntryFromReadmeIsPresent() {
    assertThat(CompatibilityMatrix.catalog())
        .extracting(CompatibilityMatrix.Entry::id)
        .containsExactlyInAnyOrder(
            "surefire/failsafe",
            "junit-platform-engine",
            "junit-platform-launcher",
            "junit-jupiter-engine");
  }

  @Test
  void givenTheShippedCatalog_whenCheckingEachEntry_thenFirstTestedNeverExceedsLastTested() {
    // Guards the one invariant hand-editing firstTested and resource-filtering
    // lastTested could silently break: a pom property rollback (or a floor raised
    // past the pin) would otherwise ship a range no version can ever satisfy.
    for (CompatibilityMatrix.Entry entry : CompatibilityMatrix.catalog()) {
      int[] first = CompatibilityMatrix.parse(entry.firstTested());
      int[] last = CompatibilityMatrix.parse(entry.lastTested());
      assertThat(first).as(entry.id() + ".firstTested parses").isNotNull();
      assertThat(last).as(entry.id() + ".lastTested parses").isNotNull();
      assertThat(CompatibilityMatrix.compare(first, last))
          .as(entry.id() + ": firstTested (%s) <= lastTested (%s)",
              entry.firstTested(), entry.lastTested())
          .isLessThanOrEqualTo(0);
    }
  }

  @Test
  void givenNoDetectedVersion_whenEvaluating_thenStaysInert() {
    assertThatCode(() -> CompatibilityMatrix.evaluate(ENTRY, null)).doesNotThrowAnyException();
  }

  @Test
  void givenAnUnparseableVersion_whenEvaluating_thenStaysInert() {
    assertThatCode(() -> CompatibilityMatrix.evaluate(ENTRY, "not-a-version"))
        .doesNotThrowAnyException();
  }

  @Test
  void givenExactlyTheTestedVersion_whenEvaluating_thenStaysInert() {
    assertThatCode(() -> CompatibilityMatrix.evaluate(ENTRY, "3.6.0")).doesNotThrowAnyException();
  }

  @Test
  void givenAVersionBelowTheFloor_whenEvaluating_thenFailsNamingEntryAndBothVersions() {
    assertThatExceptionOfType(ShardConfigurationException.class)
        .isThrownBy(() -> CompatibilityMatrix.evaluate(ENTRY, "3.5.6"))
        .withMessageContaining("surefire/failsafe")
        .withMessageContaining("3.5.6")
        .withMessageContaining("3.6.0");
  }

  @Test
  void givenAVersionAboveTheTestedOne_whenEvaluating_thenStaysInertRatherThanFailing() {
    // The warning goes through System.Logger, not a thrown exception -- an untested
    // newer version must never break a consumer's build.
    assertThatCode(() -> CompatibilityMatrix.evaluate(ENTRY, "3.6.1")).doesNotThrowAnyException();
    assertThatCode(() -> CompatibilityMatrix.evaluate(ENTRY, "4.0.0")).doesNotThrowAnyException();
  }

  @Test
  void givenAVersionWithATrailingQualifier_whenEvaluating_thenTheNumericPrefixStillParses() {
    assertThatExceptionOfType(ShardConfigurationException.class)
        .isThrownBy(() -> CompatibilityMatrix.evaluate(ENTRY, "3.5.6-SNAPSHOT"));
    assertThatCode(() -> CompatibilityMatrix.evaluate(ENTRY, "3.6.0-rc1"))
        .doesNotThrowAnyException();
  }

  @Test
  void givenARangeWiderThanOnePoint_whenTheDetectedVersionFallsInsideIt_thenStaysInert() {
    var ranged =
        new CompatibilityMatrix.Entry(
            "junit-platform-engine",
            "package-version",
            null,
            "org.junit.platform.engine.TestEngine",
            "6.1.0",
            "6.1.3");
    assertThatCode(() -> CompatibilityMatrix.evaluate(ranged, "6.1.1"))
        .doesNotThrowAnyException();
  }

  @Test
  void givenAMavenArtifactProbe_whenDetectingFromThisRealClasspath_thenFindsSurefireBooter() {
    // shard4j-engine's own reactor build forks this test under surefire, so the probe
    // this entry's config actually names must resolve on the real classpath -- not a
    // fake resource -- or the whole detection story is unverified.
    String detected = CompatibilityMatrix.detect(ENTRY);
    assertThat(detected).isNotNull();
  }

  @Test
  void givenAPackageVersionProbe_whenDetectingFromThisRealClasspath_thenFindsJUnitPlatform() {
    var entry =
        new CompatibilityMatrix.Entry(
            "junit-platform-engine",
            "package-version",
            null,
            "org.junit.platform.engine.TestEngine",
            "0.0.0",
            "999.0.0");
    assertThat(CompatibilityMatrix.detect(entry)).isNotNull();
  }

  @Test
  void givenAPackageVersionProbe_whenDetectingTheLauncher_thenFindsItOnThisRealClasspath() {
    // The launcher is provided-scope on shard4j-engine, never a compile-time reference
    // this class makes itself -- this pins that reflection alone still finds it.
    var entry =
        new CompatibilityMatrix.Entry(
            "junit-platform-launcher",
            "package-version",
            null,
            "org.junit.platform.launcher.Launcher",
            "0.0.0",
            "999.0.0");
    assertThat(CompatibilityMatrix.detect(entry)).isNotNull();
  }

  @Test
  void givenAPackageVersionProbe_whenDetectingJupiter_thenFindsItOnThisRealClasspath() {
    var entry =
        new CompatibilityMatrix.Entry(
            "junit-jupiter-engine",
            "package-version",
            null,
            "org.junit.jupiter.engine.JupiterTestEngine",
            "0.0.0",
            "999.0.0");
    assertThat(CompatibilityMatrix.detect(entry)).isNotNull();
  }

  @Test
  void givenAnUnknownProbeKind_whenDetecting_thenReturnsNullRatherThanThrowing() {
    var entry =
        new CompatibilityMatrix.Entry("mystery", "unheard-of-probe", null, null, "1.0.0", "1.0.0");
    assertThat(CompatibilityMatrix.detect(entry)).isNull();
  }
}
