package com.xgen.testing;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.Check.checkState;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import com.xgen.testing.JsonTestSuite.TestCase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Assert;

/**
 * Provides a way to test the correctness of deserialization of BSON objects into objects of type
 * {@link T}.
 *
 * <p>The logic for doing that correctness test is stored inside each {@link
 * BsonDeserializationTestSuite.TestSpec} that we pass to the {@link
 * BsonDeserializationTestSuite#runTest(TestSpec)} method.
 */
public class BsonDeserializationTestSuite<T> {

  @FunctionalInterface
  public interface BsonDeserializer<T> {
    T deserialize(BsonValue input) throws BsonParseException;
  }

  private final JsonTestSuite jsonTestSuite;
  private final BsonDeserializer<T> deserializer;

  private BsonDeserializationTestSuite(
      JsonTestSuite jsonTestSuite, BsonDeserializer<T> deserializer) {
    this.jsonTestSuite = jsonTestSuite;
    this.deserializer = deserializer;
  }

  /** Creates a BsonDeserializationTestSuite for a class that uses a FromDocumentParser. */
  public static <T> BsonDeserializationTestSuite<T> fromDocument(
      String suitePath, String suiteName, ClassField.FromDocumentParser<T> documentParser) {
    return fromDocument(suitePath, suiteName, documentParser, Optional.empty());
  }

  /**
   * Like {@link #fromDocument(String, String, ClassField.FromDocumentParser)}, but parses under the
   * given deployment environment so edition-gated on-prem fields can be validated.
   */
  public static <T> BsonDeserializationTestSuite<T> fromDocument(
      String suitePath,
      String suiteName,
      ClassField.FromDocumentParser<T> documentParser,
      Optional<DeploymentEnvironment> deploymentEnvironment) {
    return load(
        suitePath,
        suiteName,
        bsonValue -> {
          PermissiveBsonParseContext context =
              PermissiveBsonParseContext.root(deploymentEnvironment);
          T value;
          try (var parser =
              BsonDocumentParser.withContext(context, bsonValue.asDocument()).build()) {
            value = documentParser.parse(parser);
          }
          if (!context.getUnknownFieldExceptions().isEmpty()) {
            throw context.getUnknownFieldExceptions().getFirst();
          }
          return value;
        });
  }

  /** Creates a BsonDeserializationTestSuite for a class that provides a parse(Document) method. */
  public static <T> BsonDeserializationTestSuite<T> fromRootDocument(
      String suitePath,
      String suiteName,
      CheckedFunction<BsonDocument, T, BsonParseException> documentParser) {
    return load(suitePath, suiteName, bsonValue -> documentParser.apply(bsonValue.asDocument()));
  }

  /** Creates a BsonDeserializationTestSuite for a class that uses a FromValueParser. */
  public static <T> BsonDeserializationTestSuite<T> fromValue(
      String suitePath, String suiteName, ClassField.FromValueParser<T> valueParser) {
    return load(
        suitePath, suiteName, bsonValue -> valueParser.parse(BsonParseContext.root(), bsonValue));
  }

  private static <T> BsonDeserializationTestSuite<T> load(
      String suitePath, String suiteName, BsonDeserializer<T> valueParser) {
    return new BsonDeserializationTestSuite<>(
        JsonTestSuite.load(suitePath, suiteName), valueParser);
  }

  /**
   * Runs the provided {@link TestSpecWrapper}.
   *
   * <p>See {@link #withExamples(ValidSpec[])} to generate a {@link TestSpecWrapper}
   */
  public void runTest(TestSpecWrapper<T> wrapper) throws Exception {
    if (wrapper.delegate.isValid()) {
      valid(wrapper.delegate);
    } else {
      invalid(wrapper.delegate);
    }
  }

  /**
   * Returns a collection of all the provided {@link ValidSpec} instances augmented with all the
   * invalid examples declared in the json file.
   *
   * @throws AssertionError if a valid example is loaded in json but does not have a corresponding
   *     entry in the parameter list.
   */
  @SafeVarargs // Safe because array is never written to
  public final <S extends T> Collection<TestSpecWrapper<S>> withExamples(
      ValidSpec<S>... validExamples) {
    List<TestSpecWrapper<S>> specs = new ArrayList<>();

    for (ValidSpec<S> example : validExamples) {
      specs.add(new TestSpecWrapper<>(example));
    }

    // Ensure we have an example provided for every valid case declared
    ImmutableSet<String> providedTestNames =
        Arrays.stream(validExamples).map(TestSpec::getName).collect(toImmutableSet());
    ImmutableSet<String> expectedTestNames =
        this.jsonTestSuite.getValid().stream()
            .map(TestCase::getDescription)
            .collect(toImmutableSet());
    assertThat(providedTestNames).containsExactlyElementsIn(expectedTestNames);

    for (var invalid : this.jsonTestSuite.getInvalid()) {
      specs.add(new TestSpecWrapper<>(TestSpec.invalid(invalid.getDescription())));
    }
    return specs;
  }

  private void valid(TestSpec<T> testSpec) throws Exception {
    JsonTestSuite.TestCase testCase =
        this.jsonTestSuite.getValid().stream()
            .filter(t -> t.getDescription().equals(testSpec.getName()))
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        String.format(
                            "could not find valid test case \"%s\"", testSpec.getName())));

    T value = this.deserializer.deserialize(testCase.getValueAsBsonValue());
    testSpec.getValidator().accept(value);
  }

  private void invalid(TestSpec<T> testSpec) {
    JsonTestSuite.TestCase testCase =
        this.jsonTestSuite.getInvalid().stream()
            .filter(t -> t.getDescription().equals(testSpec.getName()))
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        String.format(
                            "could not find invalid test case \"%s\"", testSpec.getName())));

    BsonParseException ex =
        assertThrows(
            BsonParseException.class,
            () -> this.deserializer.deserialize(testCase.getValueAsBsonValue()));
    assertThat(ex).hasMessageThat().contains(testCase.getErrorMessageContains());
  }

  /**
   * This class does nothing but guarantee that the wrapped {@link TestSpec} was generated with
   * {@link #withExamples(ValidSpec[])}.
   */
  public static class TestSpecWrapper<T> {
    final TestSpec<T> delegate;

    private TestSpecWrapper(TestSpec<T> delegate) {
      this.delegate = delegate;
    }

    public boolean isValid() {
      return this.delegate.isValid();
    }

    @Override
    public String toString() {
      return "TestSpecWrapper(" + getName() + ")";
    }

    public String getName() {
      return this.delegate.getName();
    }
  }

  public static class ValidSpec<T> extends TestSpec<T> {

    private ValidSpec(String name, Consumer<T> validator) {
      super(name, validator);
    }
  }

  public static class InvalidSpec<T> extends TestSpec<T> {

    private InvalidSpec(String name) {
      super(name);
    }
  }

  public abstract static class TestSpec<T> {

    private final String name;
    private final Optional<Consumer<T>> validator;
    private final boolean isValid;

    private TestSpec(String name) {
      this.name = name;
      this.validator = Optional.empty();
      this.isValid = false;
    }

    private TestSpec(String name, Consumer<T> validator) {
      this.name = name;
      this.validator = Optional.of(validator);
      this.isValid = true;
    }

    public static <T> InvalidSpec<T> invalid(String name) {
      return new InvalidSpec<>(name);
    }

    public static <T> ValidSpec<T> valid(String name, Consumer<T> validator) {
      return new ValidSpec<>(name, validator);
    }

    public static <T> ValidSpec<T> valid(String name, T expected) {
      return new ValidSpec<>(
          name,
          v -> {
            Assert.assertEquals("expected object did not match result", expected, v);
            if (v instanceof Encodable encodable) {
              // Does not need to reconstruct original value, but it should not throw
              encodable.toBson();
            }
          });
    }

    private String getName() {
      return this.name;
    }

    private Consumer<T> getValidator() {
      checkState(this.isValid, "must be a valid test");
      return this.validator.orElseThrow(() -> new AssertionError("valid test must have validator"));
    }

    boolean isValid() {
      return this.isValid;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }
}
