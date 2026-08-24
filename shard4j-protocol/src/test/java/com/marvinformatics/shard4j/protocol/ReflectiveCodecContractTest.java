package com.marvinformatics.shard4j.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The codec lives in the engine, not here, so these types have to be serialisable by a
 * reflective codec with no annotations to help it. Every rule below is something such a
 * codec cannot work around.
 */
class ReflectiveCodecContractTest {

  private static final Set<Class<?>> SCALARS =
      Set.of(
          boolean.class,
          int.class,
          long.class,
          double.class,
          Boolean.class,
          Integer.class,
          Long.class,
          Double.class,
          String.class,
          Instant.class);

  static Stream<Class<?>> wireTypes() throws IOException {
    Path classes = Path.of(System.getProperty("basedir", ".")).resolve("target/classes");
    Path packageDir = classes.resolve("com/marvinformatics/shard4j/protocol");
    try (Stream<Path> files = Files.list(packageDir)) {
      return files
          .map(Path::getFileName)
          .map(Path::toString)
          .filter(name -> name.endsWith(".class"))
          .map(name -> name.substring(0, name.length() - ".class".length()))
          .filter(name -> !name.equals("package-info"))
          .map(ReflectiveCodecContractTest::load)
          .flatMap(type -> Stream.concat(Stream.of(type), Stream.of(type.getDeclaredClasses())))
          .filter(type -> Modifier.isPublic(type.getModifiers()))
          .toList()
          .stream();
    }
  }

  private static Class<?> load(String simpleName) {
    try {
      return Class.forName("com.marvinformatics.shard4j.protocol." + simpleName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  void findsTheWholeWireSurface() throws IOException {
    assertTrue(wireTypes().count() >= 20, "the scan must actually see the package");
  }

  @ParameterizedTest
  @MethodSource("wireTypes")
  void isARecordOrAnEnum(Class<?> type) {
    assertTrue(
        type.isRecord() || type.isEnum() || Throwable.class.isAssignableFrom(type),
        type.getName() + " is neither a record nor an enum");
  }

  @ParameterizedTest
  @MethodSource("wireTypes")
  void carriesNoAnnotationACodecWouldHaveToUnderstand(Class<?> type) {
    assertEquals(0, type.getAnnotations().length, type.getName() + " is annotated");
    for (RecordComponent component : components(type)) {
      assertEquals(
          0,
          component.getAnnotations().length,
          type.getSimpleName() + "." + component.getName() + " is annotated");
      assertEquals(0, component.getAccessor().getAnnotations().length, component.getName());
    }
  }

  @ParameterizedTest
  @MethodSource("wireTypes")
  void hasExactlyOneConstructorSoTheCodecCannotPickTheWrongOne(Class<?> type) {
    if (!type.isRecord()) {
      return;
    }
    assertEquals(
        1, type.getDeclaredConstructors().length, type.getName() + " has an extra constructor");
  }

  @ParameterizedTest
  @MethodSource("wireTypes")
  void namesEveryComponentInLowerCamelCase(Class<?> type) {
    for (RecordComponent component : components(type)) {
      assertTrue(
          component.getName().matches("[a-z][A-Za-z0-9]*"),
          type.getSimpleName() + "." + component.getName() + " is not lowerCamelCase");
    }
  }

  @ParameterizedTest
  @MethodSource("wireTypes")
  void buildsEveryComponentFromTypesAReflectiveCodecAlreadyKnows(Class<?> type) {
    for (RecordComponent component : components(type)) {
      assertCodecSafe(component.getGenericType(), type.getSimpleName() + "." + component.getName());
    }
  }

  @Test
  void hasNoJacksonOnItsClasspathAtAll() {
    assertThrows(
        ClassNotFoundException.class,
        () -> Class.forName("com.fasterxml.jackson.annotation.JsonProperty"),
        "shard4j-protocol must not see a JSON library, not even in test scope");
  }

  private static RecordComponent[] components(Class<?> type) {
    return type.isRecord() ? type.getRecordComponents() : new RecordComponent[0];
  }

  private static void assertCodecSafe(Type type, String where) {
    if (type instanceof Class<?> raw) {
      assertTrue(
          SCALARS.contains(raw) || raw.isEnum() || raw.isRecord(),
          where + " has type " + raw.getName() + ", which a reflective codec cannot map");
      if (raw.isEnum() || raw.isRecord()) {
        assertEquals(
            "com.marvinformatics.shard4j.protocol",
            raw.getPackageName(),
            where + " reaches outside the protocol package");
      }
      return;
    }
    if (type instanceof ParameterizedType parameterized) {
      Class<?> raw = (Class<?>) parameterized.getRawType();
      assertTrue(
          raw.equals(List.class) || raw.equals(Map.class),
          where + " has container type " + raw.getName());
      Type[] arguments = parameterized.getActualTypeArguments();
      if (raw.equals(Map.class)) {
        assertEquals(String.class, arguments[0], where + " must be keyed by String");
      }
      assertCodecSafe(arguments[arguments.length - 1], where);
      return;
    }
    assertTrue(false, where + " has type " + type + ", which a reflective codec cannot map");
  }
}
