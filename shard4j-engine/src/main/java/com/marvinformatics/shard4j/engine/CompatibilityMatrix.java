package com.marvinformatics.shard4j.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Best-effort classpath check of this release's critical build-tool dependencies, run once
 * per fork before a coordinated or direct execution starts. The tested versions live in
 * {@code compatibility.json}, shipped inside this jar and hand-edited alongside {@code
 * README.md}'s compatibility matrix -- the two must always name the same versions, and a
 * range widens only when this release's own CI has actually run against the new edge, per
 * {@code AGENTS.md}'s testing rule. Detection is per entry, by one of two probes:
 *
 * <ul>
 *   <li>{@code maven-artifact} reads {@code
 *       META-INF/maven/<groupId>/<artifactId>/pom.properties} off the classpath -- present
 *       in any jar Maven's own jar plugin built. surefire-booter is the fork's own entry
 *       point ({@code ForkedBooter}), shared by surefire and failsafe since they release
 *       from one version.
 *   <li>{@code package-version} reads {@link Package#getImplementationVersion()} off a
 *       named class's package -- for artifacts, like JUnit Platform's, that Gradle builds
 *       and so never carry a {@code pom.properties}.
 * </ul>
 *
 * <p>Either probe returning nothing (Gradle, an IDE runner, {@code forkCount=0}, a class
 * this entry names that is not on this classpath) means silence, not a warning: a consumer
 * this cannot identify is not penalised for it, matching every other corner of this
 * engine's inertness. Below {@code firstTested} is a hard failure naming the entry and the
 * detected version; above {@code lastTested} is a loud warning, not a failure -- it may
 * well work, nobody has run it yet.
 */
final class CompatibilityMatrix {

  private static final System.Logger log = System.getLogger(CompatibilityMatrix.class.getName());

  private static final String CATALOG_RESOURCE = "/shard4j/compatibility.json";

  private static final Pattern VERSION = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+).*");

  private CompatibilityMatrix() {}

  record Entry(
      String id,
      String probe,
      String coordinate,
      String probeClass,
      String firstTested,
      String lastTested) {}

  static void check() {
    for (Entry entry : catalog()) {
      evaluate(entry, detect(entry));
    }
  }

  static List<Entry> catalog() {
    try (InputStream stream = CompatibilityMatrix.class.getResourceAsStream(CATALOG_RESOURCE)) {
      if (stream == null) {
        return List.of();
      }
      return List.of(CoordinatorGateway.JSON.readValue(stream, Entry[].class));
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to read " + CATALOG_RESOURCE, e);
    }
  }

  static String detect(Entry entry) {
    return switch (entry.probe()) {
      case "maven-artifact" -> readMavenArtifactVersion(entry.coordinate());
      case "package-version" -> readPackageVersion(entry.probeClass());
      default -> null;
    };
  }

  /** Split from {@link #check()} so tests can drive one entry without a real classpath. */
  static void evaluate(Entry entry, String detected) {
    if (detected == null) {
      return;
    }
    int[] parsedDetected = parse(detected);
    int[] first = parse(entry.firstTested());
    int[] last = parse(entry.lastTested());
    if (parsedDetected == null || first == null || last == null) {
      return;
    }
    if (compare(parsedDetected, first) < 0) {
      throw new ShardConfigurationException(
          entry.id()
              + " "
              + detected
              + " is below shard4j's tested floor of "
              + entry.firstTested()
              + ". See README.md's compatibility matrix.");
    }
    if (compare(parsedDetected, last) > 0) {
      log.log(
          System.Logger.Level.WARNING,
          """

              ################################################################
              # shard4j: UNTESTED %s VERSION
              #
              # Detected %s %s. This release's own CI has only run up to %s.
              # Newer versions are not known to be broken, but nothing has
              # verified them either -- see README.md's compatibility matrix
              # before relying on this combination.
              ################################################################
              """
              .formatted(entry.id(), entry.id(), detected, entry.lastTested()));
    }
  }

  private static String readMavenArtifactVersion(String coordinate) {
    if (coordinate == null) {
      return null;
    }
    String[] parts = coordinate.split(":", 2);
    if (parts.length != 2) {
      return null;
    }
    String resource = "/META-INF/maven/" + parts[0] + "/" + parts[1] + "/pom.properties";
    try (InputStream stream = CompatibilityMatrix.class.getResourceAsStream(resource)) {
      if (stream == null) {
        return null;
      }
      Properties props = new Properties();
      props.load(stream);
      return props.getProperty("version");
    } catch (IOException | RuntimeException e) {
      return null;
    }
  }

  private static String readPackageVersion(String className) {
    if (className == null) {
      return null;
    }
    try {
      Class<?> type =
          Class.forName(className, false, CompatibilityMatrix.class.getClassLoader());
      return type.getPackage().getImplementationVersion();
    } catch (ClassNotFoundException | RuntimeException e) {
      return null;
    }
  }

  /** Package-private so a test can pin firstTested &lt;= lastTested for every entry. */
  static int[] parse(String version) {
    if (version == null) {
      return null;
    }
    Matcher matcher = VERSION.matcher(version);
    if (!matcher.matches()) {
      return null;
    }
    return new int[] {
      Integer.parseInt(matcher.group(1)),
      Integer.parseInt(matcher.group(2)),
      Integer.parseInt(matcher.group(3))
    };
  }

  static int compare(int[] left, int[] right) {
    for (int i = 0; i < 3; i++) {
      int diff = Integer.compare(left[i], right[i]);
      if (diff != 0) {
        return diff;
      }
    }
    return 0;
  }
}
