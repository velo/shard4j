package com.marvinformatics.shard4j.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Best-effort classpath check of surefire/failsafe and JUnit Platform versions against
 * {@code compatibility.json}. Errors below the tested floor, warns above the tested
 * ceiling, stays silent when it can't detect the build tool at all.
 */
final class CompatibilityMatrix {

  private static final System.Logger log = System.getLogger(CompatibilityMatrix.class.getName());

  // Own mapper, deliberately not CoordinatorGateway.JSON: that one exists for
  // wire-protocol forward-tolerance, an unrelated concern this client-only check must
  // never couple to.
  private static final ObjectMapper JSON = new ObjectMapper();

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
      return List.of(JSON.readValue(stream, Entry[].class));
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
          entry.id()
              + " "
              + detected
              + " is above shard4j's tested ceiling of "
              + entry.lastTested()
              + "; not known to be broken, but unverified. See README.md's compatibility"
              + " matrix.");
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
