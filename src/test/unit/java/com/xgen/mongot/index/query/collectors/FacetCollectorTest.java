package com.xgen.mongot.index.query.collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.query.collectors.CollectorBuilder;
import com.xgen.testing.mongot.index.query.collectors.FacetDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {FacetCollectorTest.ParameterizedSuite.class, FacetCollectorTest.RegularSuite.class})
public class FacetCollectorTest {

  @RunWith(Parameterized.class)
  public static class ParameterizedSuite {
    private static final String SUITE_NAME = "facet";
    private static final BsonDeserializationTestSuite<FacetCollector> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/index/query/collectors/",
            SUITE_NAME,
            FacetCollector::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<FacetCollector> testSpec;

    public ParameterizedSuite(
        BsonDeserializationTestSuite.TestSpecWrapper<FacetCollector> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<FacetCollector>> data() {
      return TEST_SUITE.withExamples(
          simpleStringFacet(),
          stringFacetWithNumBuckets(),
          simpleNumericFacet(),
          numericFacetWithDefaultBucket(),
          simpleDateFacet(),
          dateFacetWithDefaultBucket(),
          multipleDefinitions(),
          noOperator());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<FacetCollector> simpleStringFacet() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple string facet",
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of("directorFacet", FacetDefinitionBuilder.string().path("director").build()))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FacetCollector>
        stringFacetWithNumBuckets() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "string facet with numBuckets",
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "directorFacet",
                      FacetDefinitionBuilder.string().numBuckets(1000).path("director").build()))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FacetCollector> simpleNumericFacet() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple numeric facet",
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "ratingFacet",
                      FacetDefinitionBuilder.numeric()
                          .boundaries(List.of(new BsonInt32(1), new BsonInt32(2)))
                          .path("rating")
                          .build()))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FacetCollector>
        numericFacetWithDefaultBucket() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "numeric facet with default bucket",
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "ratingFacet",
                      FacetDefinitionBuilder.numeric()
                          .defaultBucketName("other")
                          .boundaries(List.of(new BsonInt32(1), new BsonInt32(2)))
                          .path("rating")
                          .build()))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FacetCollector> simpleDateFacet() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple date facet",
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "eventFacet",
                      FacetDefinitionBuilder.date()
                          .boundaries(
                              List.of(
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2020, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis()),
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2021, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis())))
                          .path("start")
                          .build()))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FacetCollector>
        dateFacetWithDefaultBucket() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "date facet with default bucket",
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "eventFacet",
                      FacetDefinitionBuilder.date()
                          .defaultBucketName("other")
                          .boundaries(
                              List.of(
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2020, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis()),
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2021, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis())))
                          .path("start")
                          .build()))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FacetCollector> multipleDefinitions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "multiple definitions",
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "directorFacet",
                      FacetDefinitionBuilder.string().path("director").build(),
                      "ratingFacet",
                      FacetDefinitionBuilder.numeric()
                          .defaultBucketName("other")
                          .boundaries(List.of(new BsonInt32(1), new BsonInt32(2)))
                          .path("rating")
                          .build(),
                      "eventFacet",
                      FacetDefinitionBuilder.date()
                          .defaultBucketName("other")
                          .boundaries(
                              List.of(
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2020, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis()),
                                  new BsonDateTime(
                                      new Calendar.Builder()
                                          .setDate(2021, Calendar.MAY, 8)
                                          .setTimeOfDay(12, 11, 0, 0)
                                          .setTimeZone(TimeZone.getTimeZone("UTC"))
                                          .build()
                                          .getTimeInMillis())))
                          .path("start")
                          .build()))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<FacetCollector> noOperator() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "no operator",
          CollectorBuilder.facet()
              .operator(AllDocumentsOperator.INSTANCE)
              .facetDefinitions(
                  Map.of("directorFacet", FacetDefinitionBuilder.string().path("director").build()))
              .build());
    }
  }

  public static class RegularSuite {
    @Test
    public void containsDoesNotAffect_whenOperatorWithDoesNotAffectDefined_returnsTrue() {
      var equalsOperator =
          OperatorBuilder.equals().path("field1").value("value").doesNotAffect("facet1").build();

      var inOperator =
          OperatorBuilder.in()
              .path("field2")
              .strings(Arrays.asList("value1", "value2", "value3"))
              .doesNotAffect("facet2")
              .build();

      var rangeOperator =
          OperatorBuilder.range()
              .path("field3")
              .numericBounds(
                  Optional.of(new LongPoint(10)), Optional.of(new LongPoint(20)), true, false)
              .doesNotAffect("facet3")
              .build();

      assertThat(FacetCollector.containsDoesNotAffect(equalsOperator)).isTrue();
      assertThat(FacetCollector.containsDoesNotAffect(inOperator)).isTrue();
      assertThat(FacetCollector.containsDoesNotAffect(rangeOperator)).isTrue();
    }

    @Test
    public void containsDoesNotAffect_whenOperatorWithoutDoesNotAffect_returnsFalse() {
      // Create operators without `doesNotAffect`
      var equalsOperator = OperatorBuilder.equals().path("field4").value("value").build();

      var inOperator =
          OperatorBuilder.in()
              .path("field5")
              .strings(Arrays.asList("value1", "value2", "value3"))
              .build();

      var rangeOperator =
          OperatorBuilder.range()
              .path("field3")
              .numericBounds(
                  Optional.of(new LongPoint(10)), Optional.of(new LongPoint(20)), true, false)
              .build();

      assertThat(FacetCollector.containsDoesNotAffect(equalsOperator)).isFalse();
      assertThat(FacetCollector.containsDoesNotAffect(inOperator)).isFalse();
      assertThat(FacetCollector.containsDoesNotAffect(rangeOperator)).isFalse();
    }

    @Test
    public void containsDoesNotAffect_whenCompoundOrEmbeddedReturnsTrue() {
      // Create compound and embedded operators
      var compoundOperator =
          OperatorBuilder.compound()
              .should(OperatorBuilder.equals().path("field1").value("value").build())
              .build();

      var embeddedOperator =
          OperatorBuilder.embeddedDocument()
              .path("field2")
              .operator(
                  OperatorBuilder.range()
                      .path("field3")
                      .numericBounds(
                          Optional.of(new LongPoint(1)), Optional.of(new LongPoint(3)), true, false)
                      .build())
              .build();

      // Verify that these operators always return true
      assertThat(FacetCollector.containsDoesNotAffect(compoundOperator)).isTrue();
      assertThat(FacetCollector.containsDoesNotAffect(embeddedOperator)).isTrue();
    }

    @Test
    public void containsDoesNotAffect_whenAutocompleteReturnsFalse() {
      var autocompleteOperator =
          OperatorBuilder.autocomplete().path("description").query("pizza").build();

      assertThat(FacetCollector.containsDoesNotAffect(autocompleteOperator)).isFalse();
    }

    @Test
    public void buildDrillSidewaysInfo_operatorDoesNotContainDoesNotAffect_returnsEmptyOptional() {
      long value = 1;
      Operator operator = OperatorBuilder.equals().path("color").value(value).build();

      Map<String, FacetDefinition> facetDefinitions = getFacetDefinitions();

      Optional<DrillSidewaysInfoBuilder.DrillSidewaysInfo> result =
          FacetCollector.buildDrillSidewaysInfo(operator, facetDefinitions);

      assertThat(result).isEmpty();
    }

    private static Map<String, FacetDefinition> getFacetDefinitions() {
      Map<String, FacetDefinition> facetDefinitions = new HashMap<>();
      // Add color facet definition
      facetDefinitions.put(
          "colors",
          new FacetDefinition.NumericFacetDefinition(
              "color",
              Optional.empty(),
              List.of(
                  new BsonDouble(1.0),
                  new BsonDouble(2.0),
                  new BsonDouble(3.0),
                  new BsonDouble(4.0))));

      // Add size facet definition
      facetDefinitions.put(
          "sizes",
          new FacetDefinition.NumericFacetDefinition(
              "size",
              Optional.empty(),
              List.of(
                  new BsonDouble(5.0),
                  new BsonDouble(6.0),
                  new BsonDouble(7.0),
                  new BsonDouble(8.0))));
      return facetDefinitions;
    }

    @Test
    public void numericBoundarySizeExceedingLimit() {
      var definition =
          FacetDefinitionBuilder.numeric()
              .boundaries(
                  IntStream.range(0, 2000).mapToObj(BsonInt32::new).collect(Collectors.toList()))
              .path("rating")
              .build();

      var parser = BsonDocumentParser.fromRoot(definition.toBson().asDocument()).build();
      TestUtils.assertThrows(
          "size must be within bounds",
          BsonParseException.class,
          () -> FacetDefinition.fromBson(parser));
    }

    @Test
    public void dateBoundarySizeExceedingLimit() {
      var definition =
          FacetDefinitionBuilder.date()
              .boundaries(
                  LongStream.range(0, 2000)
                      .mapToObj(v -> new BsonDateTime(new Date(v).getTime()))
                      .collect(Collectors.toList()))
              .path("rating")
              .build();

      var parser = BsonDocumentParser.fromRoot(definition.toBson().asDocument()).build();
      TestUtils.assertThrows(
          "size must be within bounds",
          BsonParseException.class,
          () -> FacetDefinition.fromBson(parser));
    }

    /** When allow10k is false (DFF off), string facet numBuckets 2000 throws BsonParseException. */
    @Test
    public void stringFacetNumBuckets2000_fromBsonWithAllow10kFalse_throwsBsonParseException() {
      var definition = FacetDefinitionBuilder.string().numBuckets(2000).path("director").build();

      var parser = BsonDocumentParser.fromRoot(definition.toBson().asDocument()).build();
      TestUtils.assertThrows(
          "must be within bounds",
          BsonParseException.class,
          () -> FacetDefinition.fromBson(parser, false));
    }

    /** When allow10k is true (DFF on), string facet numBuckets 2000 parses successfully. */
    @Test
    public void stringFacetNumBuckets2000_fromBsonWithAllow10kTrue_succeeds()
        throws BsonParseException {
      var definition = FacetDefinitionBuilder.string().numBuckets(2000).path("director").build();

      var parser = BsonDocumentParser.fromRoot(definition.toBson().asDocument()).build();
      FacetDefinition result = FacetDefinition.fromBson(parser, true);

      assertThat(result).isInstanceOf(FacetDefinition.StringFacetDefinition.class);
      assertThat(((FacetDefinition.StringFacetDefinition) result).numBuckets()).isEqualTo(2000);
      assertThat(result.path()).isEqualTo("director");
    }

    /**
     * A string facet carrying an inline {@code embeddedDocument} block round-trips through
     * toBson/fromBson, preserving the scope path and child operator.
     */
    @Test
    public void scopedStringFacet_roundTripsThroughBson() throws BsonParseException {
      var childOperator = OperatorBuilder.equals().path("variants.inStock").value(true).build();
      var scopedDefinition =
          new FacetDefinition.StringFacetDefinition(
              "variants.color",
              10,
              Optional.of(
                  new EmbeddedDocumentScope(
                      com.xgen.mongot.util.FieldPath.parse("variants"), childOperator)));

      // try-with-resources so parser.close() runs and asserts every field was consumed (the
      // no-unknown-fields invariant is only enforced on close).
      try (var parser =
          BsonDocumentParser.fromRoot(scopedDefinition.toBson().asDocument()).build()) {
        FacetDefinition result = FacetDefinition.fromBson(parser);

        assertThat(result).isEqualTo(scopedDefinition);
        assertThat(result.embeddedDocument()).isPresent();
        assertThat(result.embeddedDocument().get().path().toString()).isEqualTo("variants");
        assertThat(result.embeddedDocument().get().operator()).isEqualTo(childOperator);
      }
    }

    /** A string facet with no {@code embeddedDocument} block parses to an empty scope. */
    @Test
    public void unscopedStringFacet_hasEmptyEmbeddedDocument() throws BsonParseException {
      var definition = FacetDefinitionBuilder.string().path("director").build();

      try (var parser = BsonDocumentParser.fromRoot(definition.toBson().asDocument()).build()) {
        FacetDefinition result = FacetDefinition.fromBson(parser);

        assertThat(result.embeddedDocument()).isEmpty();
      }
    }

    /**
     * The scope's operator selects children of {@code embeddedDocument.path}, so an operator whose
     * path is not under that path is rejected at parse (mirrors {@code EmbeddedDocumentOperator}).
     */
    @Test
    public void scopedStringFacet_operatorPathNotUnderScopePath_isRejected() {
      var scopedStringFacet =
          new BsonDocument()
              .append("type", new BsonString("string"))
              .append("path", new BsonString("variants.color"))
              .append(
                  "embeddedDocument",
                  new BsonDocument()
                      .append("path", new BsonString("variants"))
                      .append(
                          "operator",
                          new BsonDocument()
                              .append(
                                  "range",
                                  new BsonDocument()
                                      .append("path", new BsonString("reviews.rating"))
                                      .append("gte", new BsonInt32(1)))));

      var parser = BsonDocumentParser.fromRoot(scopedStringFacet).build();
      TestUtils.assertThrows(
          "is not a subfield of", BsonParseException.class, () -> FacetDefinition.fromBson(parser));
    }

    /**
     * An unrecognized field on a facet (e.g. the common {@code embeddedDocuments} plural typo)
     * lists the expected fields, making the fix obvious.
     */
    @Test
    public void unrecognizedFacetField_listsExpectedFields() {
      var facetWithTypo =
          new BsonDocument()
              .append("type", new BsonString("string"))
              .append("path", new BsonString("variants.color"))
              .append(
                  "embeddedDocuments", // plural typo; the field is "embeddedDocument"
                  new BsonDocument().append("path", new BsonString("variants")));
      var collector =
          new BsonDocument()
              .append(
                  "operator",
                  new BsonDocument()
                      .append(
                          "text",
                          new BsonDocument()
                              .append("path", new BsonString("title"))
                              .append("query", new BsonString("jacket"))))
              .append("facets", new BsonDocument().append("colorFacet", facetWithTypo));

      var parser = BsonDocumentParser.fromRoot(collector).build();
      BsonParseException ex =
          TestUtils.assertThrows(
              "unrecognized field \"embeddedDocuments\"",
              BsonParseException.class,
              () -> FacetCollector.fromBson(parser));
      assertThat(ex)
          .hasMessageThat()
          .contains("Expected fields are: embeddedDocument, numBuckets, path, type");
    }

    /**
     * A number facet carrying an inline {@code embeddedDocument} block round-trips through
     * toBson/fromBson, preserving the scope path and child operator.
     */
    @Test
    public void scopedNumberFacet_roundTripsThroughBson() throws BsonParseException {
      var childOperator = OperatorBuilder.equals().path("variants.inStock").value(true).build();
      var scopedDefinition =
          new FacetDefinition.NumericFacetDefinition(
              "variants.price",
              Optional.empty(),
              List.of(new BsonInt32(0), new BsonInt32(50)),
              Optional.of(
                  new EmbeddedDocumentScope(
                      com.xgen.mongot.util.FieldPath.parse("variants"), childOperator)));

      try (var parser =
          BsonDocumentParser.fromRoot(scopedDefinition.toBson().asDocument()).build()) {
        FacetDefinition result = FacetDefinition.fromBson(parser);

        assertThat(result).isEqualTo(scopedDefinition);
        assertThat(result.embeddedDocument()).isPresent();
        assertThat(result.embeddedDocument().get().path().toString()).isEqualTo("variants");
        assertThat(result.embeddedDocument().get().operator()).isEqualTo(childOperator);
      }
    }

    /**
     * A date facet carrying an inline {@code embeddedDocument} block round-trips through
     * toBson/fromBson, preserving the scope path and child operator.
     */
    @Test
    public void scopedDateFacet_roundTripsThroughBson() throws BsonParseException {
      var childOperator = OperatorBuilder.equals().path("branches.inScope").value(true).build();
      var scopedDefinition =
          new FacetDefinition.DateFacetDefinition(
              "branches.startDate",
              Optional.empty(),
              List.of(new BsonDateTime(0), new BsonDateTime(1_000_000)),
              Optional.of(
                  new EmbeddedDocumentScope(
                      com.xgen.mongot.util.FieldPath.parse("branches"), childOperator)));

      try (var parser =
          BsonDocumentParser.fromRoot(scopedDefinition.toBson().asDocument()).build()) {
        FacetDefinition result = FacetDefinition.fromBson(parser);

        assertThat(result).isEqualTo(scopedDefinition);
        assertThat(result.embeddedDocument()).isPresent();
        assertThat(result.embeddedDocument().get().path().toString()).isEqualTo("branches");
        assertThat(result.embeddedDocument().get().operator()).isEqualTo(childOperator);
      }
    }

    /** FacetCollector.fromBson (1k limit) rejects numBuckets 2000 with BsonParseException. */
    @Test
    public void facetCollectorNumBuckets2000_fromBson_throwsBsonParseException() {
      var facetCollector =
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "directorFacet",
                      FacetDefinitionBuilder.string().numBuckets(2000).path("director").build()))
              .build();

      var parser =
          BsonDocumentParser.fromRoot(facetCollector.collectorToBson().asDocument()).build();
      TestUtils.assertThrows(
          "must be within bounds", BsonParseException.class, () -> FacetCollector.fromBson(parser));
    }

    /** When allow10k is false, number facet boundaries length 2000 throws BsonParseException. */
    @Test
    public void numericFacetBoundaries2000_fromBsonWithAllow10kFalse_throwsBsonParseException() {
      var definition =
          FacetDefinitionBuilder.numeric()
              .boundaries(
                  IntStream.range(0, 2000).mapToObj(BsonInt32::new).collect(Collectors.toList()))
              .path("rating")
              .build();

      var parser = BsonDocumentParser.fromRoot(definition.toBson().asDocument()).build();
      TestUtils.assertThrows(
          "size must be within bounds",
          BsonParseException.class,
          () -> FacetDefinition.fromBson(parser, false));
    }

    /** When allow10k is true, number facet boundaries length 2000 parses successfully. */
    @Test
    public void numericFacetBoundaries2000_fromBsonWithAllow10kTrue_succeeds()
        throws BsonParseException {
      var definition =
          FacetDefinitionBuilder.numeric()
              .boundaries(
                  IntStream.range(0, 2000).mapToObj(BsonInt32::new).collect(Collectors.toList()))
              .path("rating")
              .build();

      var parser = BsonDocumentParser.fromRoot(definition.toBson().asDocument()).build();
      FacetDefinition result = FacetDefinition.fromBson(parser, true);

      assertThat(result).isInstanceOf(FacetDefinition.NumericFacetDefinition.class);
      assertThat(((FacetDefinition.NumericFacetDefinition) result).boundaries()).hasSize(2000);
    }

    /** When allow10k is false, date facet boundaries length 2000 throws BsonParseException. */
    @Test
    public void dateFacetBoundaries2000_fromBsonWithAllow10kFalse_throwsBsonParseException() {
      var definition =
          FacetDefinitionBuilder.date()
              .boundaries(
                  LongStream.range(0, 2000)
                      .mapToObj(v -> new BsonDateTime(new Date(v).getTime()))
                      .collect(Collectors.toList()))
              .path("eventDate")
              .build();

      var parser = BsonDocumentParser.fromRoot(definition.toBson().asDocument()).build();
      TestUtils.assertThrows(
          "size must be within bounds",
          BsonParseException.class,
          () -> FacetDefinition.fromBson(parser, false));
    }

    /** When allow10k is true, date facet boundaries length 2000 parses successfully. */
    @Test
    public void dateFacetBoundaries2000_fromBsonWithAllow10kTrue_succeeds()
        throws BsonParseException {
      var definition =
          FacetDefinitionBuilder.date()
              .boundaries(
                  LongStream.range(0, 2000)
                      .mapToObj(v -> new BsonDateTime(new Date(v).getTime()))
                      .collect(Collectors.toList()))
              .path("eventDate")
              .build();

      var parser = BsonDocumentParser.fromRoot(definition.toBson().asDocument()).build();
      FacetDefinition result = FacetDefinition.fromBson(parser, true);

      assertThat(result).isInstanceOf(FacetDefinition.DateFacetDefinition.class);
      assertThat(((FacetDefinition.DateFacetDefinition) result).boundaries()).hasSize(2000);
    }

    /** FacetCollector.fromBson10kAllowed accepts number facet with 2000 boundaries. */
    @Test
    public void facetCollectorNumericBoundaries2000_fromBson10kAllowed_succeeds()
        throws BsonParseException {
      List<BsonNumber> boundaries =
          IntStream.range(0, 2000)
              .mapToObj(i -> (BsonNumber) new BsonInt32(i))
              .collect(Collectors.toList());
      var facetCollector =
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "ratingFacet",
                      FacetDefinitionBuilder.numeric()
                          .boundaries(boundaries)
                          .path("rating")
                          .build()))
              .build();

      var parser =
          BsonDocumentParser.fromRoot(facetCollector.collectorToBson().asDocument()).build();
      FacetCollector result = FacetCollector.fromBson10kAllowed(parser);

      FacetDefinition.NumericFacetDefinition ratingFacet =
          (FacetDefinition.NumericFacetDefinition) result.facetDefinitions().get("ratingFacet");
      assertThat(ratingFacet).isNotNull();
      assertThat(ratingFacet.boundaries()).hasSize(2000);
    }

    /** FacetCollector.fromBson10kAllowed accepts numBuckets 2000. */
    @Test
    public void facetCollectorNumBuckets2000_fromBson10kAllowed_succeeds()
        throws BsonParseException {
      var facetCollector =
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("review").query("good").build())
              .facetDefinitions(
                  Map.of(
                      "directorFacet",
                      FacetDefinitionBuilder.string().numBuckets(2000).path("director").build()))
              .build();

      var parser =
          BsonDocumentParser.fromRoot(facetCollector.collectorToBson().asDocument()).build();
      FacetCollector result = FacetCollector.fromBson10kAllowed(parser);

      FacetDefinition.StringFacetDefinition directorFacet =
          (FacetDefinition.StringFacetDefinition) result.facetDefinitions().get("directorFacet");
      assertThat(directorFacet).isNotNull();
      assertThat(directorFacet.numBuckets()).isEqualTo(2000);
    }

    @Test
    public void getTotalRequestedFacetBuckets_sumsAllFacetTypes() {
      FacetCollector oneString =
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("q").query("x").build())
              .facetDefinitions(
                  Map.of("a", FacetDefinitionBuilder.string().numBuckets(100).path("f1").build()))
              .build();
      assertThat(oneString.getTotalRequestedFacetBuckets()).isEqualTo(100);

      FacetCollector numericOnly =
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("q").query("x").build())
              .facetDefinitions(
                  Map.of(
                      "n",
                      FacetDefinitionBuilder.numeric()
                          .boundaries(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)))
                          .path("f2")
                          .build()))
              .build();
      assertThat(numericOnly.getTotalRequestedFacetBuckets()).isEqualTo(2);

      FacetCollector stringAndNumeric =
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("q").query("x").build())
              .facetDefinitions(
                  Map.of(
                      "s",
                      FacetDefinitionBuilder.string().numBuckets(50).path("f1").build(),
                      "n",
                      FacetDefinitionBuilder.numeric()
                          .boundaries(List.of(new BsonInt32(1), new BsonInt32(2)))
                          .path("f2")
                          .build()))
              .build();
      assertThat(stringAndNumeric.getTotalRequestedFacetBuckets()).isEqualTo(51);

      FacetCollector mixed =
          CollectorBuilder.facet()
              .operator(OperatorBuilder.text().path("q").query("x").build())
              .facetDefinitions(
                  Map.of(
                      "s",
                      FacetDefinitionBuilder.string().numBuckets(10).path("f1").build(),
                      "n",
                      FacetDefinitionBuilder.numeric()
                          .boundaries(List.of(new BsonInt32(1), new BsonInt32(2)))
                          .path("f2")
                          .build(),
                      "d",
                      FacetDefinitionBuilder.date()
                          .boundaries(
                              List.of(
                                  new BsonDateTime(1L), new BsonDateTime(2L), new BsonDateTime(3L)))
                          .path("f3")
                          .build()))
              .build();
      assertThat(mixed.getTotalRequestedFacetBuckets()).isEqualTo(13);
    }
  }
}
