package com.marvinformatics.shard4j.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

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
  void findsTheWholeWireSurface() throws Exception {
    assertThat(wireTypes().count() >= 20).as("the scan must actually see the package").isTrue();
  }

  @ParameterizedTest
  @MethodSource("wireTypes")
  void isARecordOrAnEnum(Class<?> type) {
    assertThat(type.isRecord() || type.isEnum() || Throwable.class.isAssignableFrom(type))
        .as(type.getName() + " is neither a record nor an enum")
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("wireTypes")
  void carriesNoAnnotationACodecWouldHaveToUnderstand(Class<?> type) {
    assertThat(type.getAnnotations().length).as(type.getName() + " is annotated").isZero();
    for (RecordComponent component : components(type)) {
      assertThat(component.getAnnotations().length)
          .as(type.getSimpleName() + "." + component.getName() + " is annotated")
          .isZero();
      assertThat(component.getAccessor().getAnnotations().length).as(component.getName()).isZero();
    }
  }

  @ParameterizedTest
  @MethodSource("wireTypes")
  void hasExactlyOneConstructorSoTheCodecCannotPickTheWrongOne(Class<?> type) {
    if (!type.isRecord()) {
      return;
    }
    assertThat(type.getDeclaredConstructors().length)
        .as(type.getName() + " has an extra constructor")
        .isOne();
  }

  @ParameterizedTest
  @MethodSource("wireTypes")
  void namesEveryComponentInLowerCamelCase(Class<?> type) {
    for (RecordComponent component : components(type)) {
      assertThat(component.getName())
          .as(type.getSimpleName() + "." + component.getName() + " is not lowerCamelCase")
          .matches("[a-z][A-Za-z0-9]*");
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
    assertThatExceptionOfType(ClassNotFoundException.class)
        .as("shard4j-protocol must not see a JSON library, not even in test scope")
        .isThrownBy(() -> Class.forName("com.fasterxml.jackson.annotation.JsonProperty"));
  }

  private static RecordComponent[] components(Class<?> type) {
    return type.isRecord() ? type.getRecordComponents() : new RecordComponent[0];
  }

  private static void assertCodecSafe(Type type, String where) {
    if (type instanceof Class<?> raw) {
      assertThat(SCALARS.contains(raw) || raw.isEnum() || raw.isRecord())
          .as(where + " has type " + raw.getName() + ", which a reflective codec cannot map")
          .isTrue();
      if (raw.isEnum() || raw.isRecord()) {
        assertThat(raw.getPackageName())
            .as(where + " reaches outside the protocol package")
            .isEqualTo("com.marvinformatics.shard4j.protocol");
      }
      return;
    }
    if (type instanceof ParameterizedType parameterized) {
      Class<?> raw = (Class<?>) parameterized.getRawType();
      assertThat(raw.equals(List.class) || raw.equals(Map.class))
          .as(where + " has container type " + raw.getName())
          .isTrue();
      Type[] arguments = parameterized.getActualTypeArguments();
      if (raw.equals(Map.class)) {
        assertThat(arguments[0]).as(where + " must be keyed by String").isEqualTo(String.class);
      }
      assertCodecSafe(arguments[arguments.length - 1], where);
      return;
    }
    fail(where + " has type " + type + ", which a reflective codec cannot map");
  }
}
