package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.StoredSourceDefinition.Mode.EXCLUSION;
import static com.xgen.mongot.index.definition.StoredSourceDefinition.Mode.INCLUSION;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertEquals;

import com.google.common.truth.Truth;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.analyzer.custom.IcuNormalizerTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.analyzer.custom.CustomCharFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.CustomAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.BooleanFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.ObjectIdFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TypeSetDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.sort.SortFieldBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.mongot.index.query.sort.UserFieldSortOptionsBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      SearchIndexDefinitionTest.DeserializationTest.class,
      SearchIndexDefinitionTest.SerializationTest.class,
      SearchIndexDefinitionTest.DefinitionTest.class,
    })
public class SearchIndexDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "search-index-deserialization";
    private static final BsonDeserializationTestSuite<SearchIndexDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, SearchIndexDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<SearchIndexDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<SearchIndexDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<SearchIndexDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          simple(),
          withAnalyzer(),
          withTypeSets(),
          withSort(),
          withNestedTypeSets(),
          withUnreferencedTypeSets(),
          withSearchAnalyzer(),
          withCustomAnalyzer(),
          withSynonyms(),
          withView(),
          withStoredBoolean(),
          withStoredDocumentInclusion(),
          withStoredDocumentExclusion(),
          withHighIndexFeatureVersion(),
          withDefaultNumPartitions(),
          withMultipleNumPartitions(),
          withIndexIdAtCreationTime(),
          withIndexIdAtCreationTimeSameAsIndexId(),
          withAutoEmbeddingDefinitionVersion(),
          withMaterializedViewNameFormatVersion(),
          withBothAutoEmbeddingAndMaterializedViewVersions(),
          withMultiTypeSortField());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition> withAnalyzer() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with analyzer",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .analyzerName("my-analyzer")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withUnreferencedTypeSets() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with unreferenced typeSets",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .analyzerName("my-analyzer")
              .typeSets(
                  TypeSetDefinitionBuilder.builder()
                      .name("foo")
                      .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
                      .build(),
                  TypeSetDefinitionBuilder.builder()
                      .name("bar")
                      .addType(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition> withTypeSets() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with typeSets",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic("foo").build())
              .analyzerName("my-analyzer")
              .typeSets(
                  TypeSetDefinitionBuilder.builder()
                      .name("foo")
                      .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
                      .build(),
                  TypeSetDefinitionBuilder.builder()
                      .name("bar")
                      .addType(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition> withSort() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with sort",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .dynamic(false)
                      .field(
                          "a",
                          FieldDefinitionBuilder.builder()
                              .token(TokenFieldDefinitionBuilder.builder().build())
                              .build())
                      .field(
                          "b",
                          FieldDefinitionBuilder.builder()
                              .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                              .build())
                      .field(
                          "c",
                          FieldDefinitionBuilder.builder()
                              .date(DateFieldDefinitionBuilder.builder().build())
                              .build())
                      .field(
                          "d",
                          FieldDefinitionBuilder.builder()
                              .objectid(ObjectIdFieldDefinitionBuilder.builder().build())
                              .build())
                      .field(
                          "e",
                          FieldDefinitionBuilder.builder()
                              .document(
                                  DocumentFieldDefinitionBuilder.builder()
                                      .dynamic(false)
                                      .field(
                                          "f",
                                          FieldDefinitionBuilder.builder()
                                              .token(TokenFieldDefinitionBuilder.builder().build())
                                              .build())
                                      .build())
                              .embeddedDocuments(
                                  EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                      .dynamic(true)
                                      .build())
                              .build())
                      .build())
              .analyzerName("my-analyzer")
              .sort(
                  SortSpecBuilder.builder()
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("a")
                              .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                              .build())
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("b")
                              .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                              .build())
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("c")
                              .sortOption(
                                  new UserFieldSortOptionsBuilder()
                                      .sortOrder(SortOrder.ASC)
                                      .nullEmptySortPosition(NullEmptySortPosition.HIGHEST)
                                      .build())
                              .build())
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("d")
                              .sortOption(
                                  new UserFieldSortOptionsBuilder()
                                      .sortOrder(SortOrder.DESC)
                                      .nullEmptySortPosition(NullEmptySortPosition.LOWEST)
                                      .build())
                              .build())
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("e.f")
                              .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                              .build())
                      .buildSort())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withNestedTypeSets() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with nested typeSets",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .dynamic("foo")
                      .field(
                          "nested",
                          FieldDefinitionBuilder.builder()
                              .document(
                                  DocumentFieldDefinitionBuilder.builder().dynamic("bar").build())
                              .build())
                      .build())
              .analyzerName("my-analyzer")
              .typeSets(
                  TypeSetDefinitionBuilder.builder()
                      .name("foo")
                      .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
                      .build(),
                  TypeSetDefinitionBuilder.builder()
                      .name("bar")
                      .addType(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withSearchAnalyzer() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with searchAnalyzer",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .searchAnalyzerName("my-search-analyzer")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withCustomAnalyzer() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with custom analyzer",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .analyzers(
                  List.of(
                      CustomAnalyzerDefinitionBuilder.builder(
                              "simple",
                              TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
                          .charFilter(
                              CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder()
                                  .ignoredTag("a")
                                  .build())
                          .tokenFilter(
                              TokenFilterDefinitionBuilder.LengthTokenFilter.builder()
                                  .max(2)
                                  .build())
                          .build()))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition> withView() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with view",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .view(
                  ViewDefinition.existing(
                      "myView",
                      List.of(
                          new BsonDocument("$set", new BsonDocument("cats", new BsonInt32(20))),
                          new BsonDocument("$set", new BsonDocument("dogs", new BsonInt32(40))))))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition> withSynonyms() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with synonyms",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .synonyms(
                  SynonymMappingDefinitionBuilder.builder()
                      .name("synonyms")
                      .synonymSourceDefinition("synonymCollection")
                      .analyzer("lucene.english")
                      .buildAsList())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withStoredBoolean() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with stored fields boolean",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .storedSource(StoredSourceDefinition.createIncludeAll())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withStoredDocumentInclusion() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with stored fields document inclusion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .storedSource(StoredSourceDefinition.create(INCLUSION, List.of("a", "b.c", "b.d")))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withStoredDocumentExclusion() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with stored fields document exclusion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .storedSource(StoredSourceDefinition.create(EXCLUSION, List.of("a", "b.c", "b.d")))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withHighIndexFeatureVersion() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with high indexFeatureVersion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withDefaultNumPartitions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with default numPartitions",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .numPartitions(1)
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withMultipleNumPartitions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with multiple numPartitions",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .numPartitions(2)
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withIndexIdAtCreationTime() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with indexIdAtCreationTime",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .indexIdAtCreationTime(new ObjectId("607f191e810c19729de860eb"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withIndexIdAtCreationTimeSameAsIndexId() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with indexIdAtCreationTime same as indexId",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .indexIdAtCreationTime(new ObjectId("507f191e810c19729de860ea"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withAutoEmbeddingDefinitionVersion() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with autoEmbeddingDefinitionVersion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .autoEmbeddingDefinitionVersion(5L)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withMaterializedViewNameFormatVersion() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with materializedViewNameFormatVersion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .materializedViewNameFormatVersion(2L)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withBothAutoEmbeddingAndMaterializedViewVersions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with both autoEmbeddingDefinitionVersion and materializedViewNameFormatVersion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .autoEmbeddingDefinitionVersion(5L)
              .materializedViewNameFormatVersion(2L)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<SearchIndexDefinition>
        withMultiTypeSortField() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "multi-type sort field with token and number",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .dynamic(false)
                      .field(
                          "a",
                          FieldDefinitionBuilder.builder()
                              .token(TokenFieldDefinitionBuilder.builder().build())
                              .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                              .build())
                      .build())
              .sort(
                  SortSpecBuilder.builder()
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("a")
                              .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                              .build())
                      .buildSort())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "search-index-serialization";
    private static final BsonSerializationTestSuite<SearchIndexDefinition> TEST_SUITE =
        fromEncodable(DefinitionTests.RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<SearchIndexDefinition>> data() {
      return Arrays.asList(
          simple(),
          withAnalyzer(),
          withTypeSets(),
          withSort(),
          withSearchAnalyzer(),
          withCustomAnalyzer(),
          withView(),
          withStoredBoolean(),
          withStoredDocumentInclusion(),
          withStoredDocumentExclusion(),
          withMultipleIndexPartitions(),
          withIndexIdAtCreationTime(),
          withAutoEmbeddingDefinitionVersion(),
          withMaterializedViewNameFormatVersion(),
          withBothAutoEmbeddingAndMaterializedViewVersions());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> withAnalyzer() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with analyzer",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .analyzerName("my-analyzer")
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> withTypeSets() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with typeSets",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .analyzerName("my-analyzer")
              .typeSets(
                  TypeSetDefinitionBuilder.builder()
                      .name("foo")
                      .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
                      .build(),
                  TypeSetDefinitionBuilder.builder()
                      .name("bar")
                      .addType(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> withSort() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with sort",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .dynamic(false)
                      .field(
                          "a",
                          FieldDefinitionBuilder.builder()
                              .token(TokenFieldDefinitionBuilder.builder().build())
                              .build())
                      .field(
                          "b",
                          FieldDefinitionBuilder.builder()
                              .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                              .build())
                      .field(
                          "c",
                          FieldDefinitionBuilder.builder()
                              .date(DateFieldDefinitionBuilder.builder().build())
                              .build())
                      .field(
                          "d",
                          FieldDefinitionBuilder.builder()
                              .objectid(ObjectIdFieldDefinitionBuilder.builder().build())
                              .build())
                      .build())
              .analyzerName("my-analyzer")
              .sort(
                  SortSpecBuilder.builder()
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("a")
                              .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                              .build())
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("b")
                              .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                              .build())
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("c")
                              .sortOption(
                                  new UserFieldSortOptionsBuilder()
                                      .sortOrder(SortOrder.ASC)
                                      .nullEmptySortPosition(NullEmptySortPosition.HIGHEST)
                                      .build())
                              .build())
                      .sortField(
                          SortFieldBuilder.builder()
                              .path("d")
                              .sortOption(
                                  new UserFieldSortOptionsBuilder()
                                      .sortOrder(SortOrder.DESC)
                                      .nullEmptySortPosition(NullEmptySortPosition.LOWEST)
                                      .build())
                              .build())
                      .buildSort())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> withSearchAnalyzer() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with searchAnalyzer",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .searchAnalyzerName("my-search-analyzer")
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> withCustomAnalyzer() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with custom analyzer",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .analyzers(
                  List.of(
                      CustomAnalyzerDefinitionBuilder.builder(
                              "persian",
                              TokenizerDefinitionBuilder.WhitespaceTokenizer.builder().build())
                          .charFilter(CustomCharFilterDefinitionBuilder.PersianCharFilter.build())
                          .tokenFilter(
                              TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
                                  .normalizationForm(
                                      IcuNormalizerTokenFilterDefinition.NormalizationForm.NFKC)
                                  .build())
                          .build()))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> withView() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with view",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .view(
                  ViewDefinition.existing(
                      "myView",
                      List.of(
                          new BsonDocument("$set", new BsonDocument("cats", new BsonInt32(20))),
                          new BsonDocument("$set", new BsonDocument("dogs", new BsonInt32(40))))))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition> withStoredBoolean() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with stored fields boolean",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .storedSource(StoredSourceDefinition.createIncludeAll())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition>
        withStoredDocumentInclusion() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with stored fields document inclusion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .storedSource(StoredSourceDefinition.create(INCLUSION, List.of("a", "b.c", "b.d")))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition>
        withStoredDocumentExclusion() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with stored fields document exclusion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .storedSource(StoredSourceDefinition.create(EXCLUSION, List.of("a", "b.c", "b.d")))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition>
        withMultipleIndexPartitions() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with multiple index-partitions",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .numPartitions(2)
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition>
        withIndexIdAtCreationTime() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with indexIdAtCreationTime",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .indexIdAtCreationTime(new ObjectId("607f191e810c19729de860eb"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition>
        withAutoEmbeddingDefinitionVersion() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with autoEmbeddingDefinitionVersion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .autoEmbeddingDefinitionVersion(5L)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition>
        withMaterializedViewNameFormatVersion() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with materializedViewNameFormatVersion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .materializedViewNameFormatVersion(2L)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<SearchIndexDefinition>
        withBothAutoEmbeddingAndMaterializedViewVersions() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with both autoEmbeddingDefinitionVersion and materializedViewNameFormatVersion",
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .autoEmbeddingDefinitionVersion(5L)
              .materializedViewNameFormatVersion(2L)
              .build());
    }
  }

  public static class DefinitionTest {

    private static final ObjectId BASIC_INDEX_ID = new ObjectId("507f191e810c19729de860ea");
    private static final UUID BASIC_COLLECTION_UUID =
        UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa");
    private static final DocumentFieldDefinition BASIC_MAPPINGS =
        DocumentFieldDefinitionBuilder.builder().dynamic(true).build();
    private static final SearchIndexDefinitionBuilder BASIC_DEFINITION_BUILDER =
        SearchIndexDefinitionBuilder.builder()
            .indexId(BASIC_INDEX_ID)
            .name("name")
            .database("database")
            .lastObservedCollectionName("collection")
            .collectionUuid(BASIC_COLLECTION_UUID)
            .mappings(BASIC_MAPPINGS);

    @Test
    public void fromBson_autoEmbedField_roundTripsCanonicalSyntax() throws Exception {
      BsonDocument bson =
          definitionBuilderWithMappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .dynamic(false)
                      .field(
                          "embedding",
                          FieldDefinitionBuilder.builder()
                              .searchAutoEmbed(
                                  new SearchAutoEmbedFieldDefinition(
                                      "voyage-3-large", FieldPath.parse("content")))
                              .build())
                      .build())
              .build()
              .toBson();

      SearchIndexDefinition parsed = SearchIndexDefinition.fromBson(bson);

      Assert.assertEquals(bson, parsed.toBson());
      Assert.assertEquals(
          new BsonString("autoEmbed"),
          parsed
              .toBson()
              .getDocument("mappings")
              .getDocument("fields")
              .getDocument("embedding")
              .get("type"));
    }

    @Test
    public void isAutoEmbeddingIndex_nestedAutoEmbedOnly_returnsTrue() {
      // The auto-embedding pipeline (MV generation, query embedding) is gated on this flag; an
      // auto-embed field declared under a `document` field must be recognized.
      var def = definitionBuilderWithMappings(nestedAutoEmbedMappings()).build();
      Assert.assertTrue(def.isAutoEmbeddingIndex());
      Assert.assertEquals(2, def.getParsedAutoEmbeddingFeatureVersion());
    }

    @Test
    public void getModelNamePerPath_mixedTopLevelAndNestedAutoEmbed_containsBothPaths() {
      // EmbeddingIndexingWorkScheduler builds per-path embed configs from this map; a nested
      // declaration's source field must appear or its values are never embedded.
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "title_embedding",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("title")))
                      .build())
              .field("parent", nestedAutoEmbedField())
              .build();
      var def = definitionBuilderWithMappings(mappings).build();
      Assert.assertEquals(
          Map.of(
              FieldPath.parse("title"), "voyage-3-large",
              FieldPath.parse("content"), "voyage-3-large"),
          def.getModelNamePerPath());
    }

    @Test
    public void getModelNamePerPath_autoEmbedAndDocumentAlternatives_collectsBoth() {
      // A field declared as both an auto-embed and a document with its own nested auto-embed
      // (search mappings may declare several types per field): both source fields must be
      // collected or the nested one is never embedded.
      DocumentFieldDefinition nested =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "nested_embedding",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("deep")))
                      .build())
              .build();
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "combo",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("top")))
                      .document(nested)
                      .build())
              .build();
      var def = definitionBuilderWithMappings(mappings).build();
      Assert.assertEquals(
          Map.of(
              FieldPath.parse("top"), "voyage-3-large",
              FieldPath.parse("deep"), "voyage-3-large"),
          def.getModelNamePerPath());
    }

    @Test
    public void construction_duplicateAutoEmbedPaths_keepsLastWithoutThrowing() {
      // Duplicate paths are rejected at the definition boundaries (fromBson and the
      // community validator); construction itself must not throw a raw duplicate-key error.
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "first_embed",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("content")))
                      .build())
              .field("parent", nestedAutoEmbedField())
              .build();
      var def = definitionBuilderWithMappings(mappings).build();
      Assert.assertEquals(
          Map.of(FieldPath.parse("content"), "voyage-3-large"), def.getModelNamePerPath());
    }

    @Test
    public void fromBson_duplicateAutoEmbedPaths_throwsParseError() {
      // A nested and a top-level declaration sharing a path must fail as a clean parse
      // error rather than an IllegalArgumentException from map construction.
      var def = definitionBuilderWithMappings(nestedAutoEmbedMappings()).build();
      BsonDocument bson = def.toBson();
      bson.getDocument("mappings")
          .getDocument("fields")
          .put(
              "duplicate_embed",
              BsonDocument.parse(
                  "{'type': 'autoEmbed', 'model': 'voyage-3-large',"
                      + " 'modality': 'text', 'path': 'content'}"));
      try {
        SearchIndexDefinition.fromBson(bson);
        Assert.fail("expected BsonParseException");
      } catch (BsonParseException e) {
        Assert.assertTrue(
            "message should name the duplicate path: " + e.getMessage(),
            e.getMessage().contains("path") && e.getMessage().contains("content"));
      }
    }

    @Test
    public void fromBson_pathOverlapsDeclaredTextField_isAccepted() throws Exception {
      // A path may be both an autoEmbed source and an explicitly declared text field (dual role):
      // the pipeline embeds the source value and the MV copies the raw value so the text mapping is
      // populated.
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "content",
                  FieldDefinitionBuilder.builder()
                      .string(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .field(
                  "content_embed",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("content")))
                      .build())
              .build();
      BsonDocument bson = definitionBuilderWithMappings(mappings).build().toBson();

      SearchIndexDefinition parsed = SearchIndexDefinition.fromBson(bson);

      Assert.assertTrue(parsed.getMappings().fields().containsKey("content"));
      Assert.assertEquals(
          FieldPath.parse("content"),
          parsed
              .getMappings()
              .fields()
              .get("content_embed")
              .searchAutoEmbedFieldDefinition()
              .orElseThrow()
              .path());
    }

    @Test
    public void fromBson_pathOverlapsOwnScalarAlternative_isAccepted() throws Exception {
      // Dual role: one declaration may carry a text alternative and an autoEmbed; the scalar and
      // the generated vector share the field key as their query path.
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "title",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("title")))
                      .string(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .build();
      BsonDocument bson = definitionBuilderWithMappings(mappings).build().toBson();

      SearchIndexDefinition parsed = SearchIndexDefinition.fromBson(bson);

      FieldDefinition parsedTitle = parsed.getMappings().fields().get("title");
      Assert.assertTrue(parsedTitle.stringFieldDefinition().isPresent());
      Assert.assertEquals(
          FieldPath.parse("title"),
          parsedTitle.searchAutoEmbedFieldDefinition().orElseThrow().path());
    }

    @Test
    public void fromBson_autoEmbedPathEqualsFieldNameOnlyType_isAccepted() throws Exception {
      // The auto-embed field name may equal its own source path as long as the declaration has no
      // other type alternative.
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "description",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("description")))
                      .build())
              .build();
      BsonDocument bson = definitionBuilderWithMappings(mappings).build().toBson();

      SearchIndexDefinition parsed = SearchIndexDefinition.fromBson(bson);

      Assert.assertEquals(
          FieldPath.parse("description"),
          parsed
              .getMappings()
              .fields()
              .get("description")
              .searchAutoEmbedFieldDefinition()
              .orElseThrow()
              .path());
    }

    @Test
    public void collectDeclaredPathsAndAutoEmbedPaths_scalarFieldSharingSourcePath_isNotConflict() {
      // Dual role: the scalar mapping is a separate field, so the shared source path is allowed.
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "description",
                  FieldDefinitionBuilder.builder()
                      .string(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .field(
                  "desc_embedding",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("description")))
                      .build())
              .build();

      CollectedPaths collected = collectPaths(mappings);

      assertEquals(Set.of(), collected.conflictingPaths());
      assertEquals(Set.of(FieldPath.parse("description")), collected.autoEmbedPaths());
    }

    @Test
    public void collectDeclaredPathsAndAutoEmbedPaths_autoEmbedOnOwnPathWithScalar_isNotConflict() {
      // Dual role: a text alternative on the auto-embed source path is allowed.
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "title",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("title")))
                      .string(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .build();

      CollectedPaths collected = collectPaths(mappings);

      assertEquals(Set.of(), collected.conflictingPaths());
      assertEquals(Set.of(FieldPath.parse("title")), collected.autoEmbedPaths());
    }

    @Test
    public void collectDeclaredPathsAndAutoEmbedPaths_autoEmbedOnOwnPathWithNumber_isNotConflict() {
      // Scalar alternatives are allowed alongside auto-embed on the same field.
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "description",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("description")))
                      .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                      .build())
              .build();

      CollectedPaths collected = collectPaths(mappings);

      assertEquals(Set.of(), collected.conflictingPaths());
      assertEquals(Set.of(FieldPath.parse("description")), collected.autoEmbedPaths());
    }

    @Test
    public void collectDeclaredPathsAndAutoEmbedPaths_scalarUnderEmbeddedDocuments_isConflict() {
      // Auto-embed beneath embeddedDocuments is unsupported (AUTO_EMBED is an illegal
      // embeddedDocuments subfield type), so an auto-embed path pointing into the subtree stays
      // conflicting even when the leaf itself is a scalar.
      EmbeddedDocumentsFieldDefinition embedded =
          EmbeddedDocumentsFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "comment",
                  FieldDefinitionBuilder.builder()
                      .string(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .build();
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "reviews", FieldDefinitionBuilder.builder().embeddedDocuments(embedded).build())
              .field(
                  "reviews_embed",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("reviews.comment")))
                      .build())
              .build();

      CollectedPaths collected = collectPaths(mappings);

      assertEquals(Set.of(FieldPath.parse("reviews.comment")), collected.conflictingPaths());
      assertEquals(Set.of(FieldPath.parse("reviews.comment")), collected.autoEmbedPaths());
    }

    @Test
    public void
        collectDeclaredPathsAndAutoEmbedPaths_nestedScalarSharingSourcePath_isNotConflict() {
      // Only embeddedDocuments-subtree paths are collected; a nested scalar sharing the source
      // path is a plain dual role and stays allowed.
      DocumentFieldDefinition parent =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "title",
                  FieldDefinitionBuilder.builder()
                      .string(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .field(
                  "title_embedding",
                  FieldDefinitionBuilder.builder()
                      .searchAutoEmbed(
                          new SearchAutoEmbedFieldDefinition(
                              "voyage-3-large", FieldPath.parse("parent.title")))
                      .build())
              .build();
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field("parent", FieldDefinitionBuilder.builder().document(parent).build())
              .build();

      CollectedPaths collected = collectPaths(mappings);

      assertEquals(Set.of(), collected.conflictingPaths());
      assertEquals(Set.of(FieldPath.parse("parent.title")), collected.autoEmbedPaths());
    }

    private record CollectedPaths(Set<FieldPath> conflictingPaths, Set<FieldPath> autoEmbedPaths) {}

    private static CollectedPaths collectPaths(DocumentFieldDefinition mappings) {
      Set<FieldPath> conflictingPaths = new HashSet<>();
      Set<FieldPath> autoEmbedPaths = new LinkedHashSet<>();
      SearchIndexDefinition.collectDeclaredPathsAndAutoEmbedPaths(
          mappings.fields(), Optional.empty(), conflictingPaths, autoEmbedPaths);
      return new CollectedPaths(conflictingPaths, autoEmbedPaths);
    }

    /**
     * Fresh builder per test: {@code BASIC_DEFINITION_BUILDER} is shared and mutable, so setting
     * mappings on it would leak into tests that assert the default {@code BASIC_MAPPINGS}.
     */
    private static SearchIndexDefinitionBuilder definitionBuilderWithMappings(
        DocumentFieldDefinition mappings) {
      return SearchIndexDefinitionBuilder.builder()
          .indexId(BASIC_INDEX_ID)
          .name("name")
          .database("database")
          .lastObservedCollectionName("collection")
          .collectionUuid(BASIC_COLLECTION_UUID)
          .mappings(mappings);
    }

    private static DocumentFieldDefinition nestedAutoEmbedMappings() {
      return DocumentFieldDefinitionBuilder.builder()
          .dynamic(false)
          .field("parent", nestedAutoEmbedField())
          .build();
    }

    private static FieldDefinition nestedAutoEmbedField() {
      return FieldDefinitionBuilder.builder()
          .document(
              DocumentFieldDefinitionBuilder.builder()
                  .dynamic(false)
                  .field(
                      "embedding",
                      FieldDefinitionBuilder.builder()
                          .searchAutoEmbed(
                              new SearchAutoEmbedFieldDefinition(
                                  "voyage-3-large", FieldPath.parse("content")))
                          .build())
                  .build())
          .build();
    }

    @Test
    public void typeSet_differs_notEqual() {
      var typeSets1 =
          TypeSetDefinitionBuilder.builder()
              .name("foo")
              .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
              .build();

      var typeSets2 =
          TypeSetDefinitionBuilder.builder()
              .name("bar")
              .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
              .build();

      var def1 = BASIC_DEFINITION_BUILDER.typeSets(typeSets1).build();
      var def2 = BASIC_DEFINITION_BUILDER.typeSets(typeSets2).build();

      Assert.assertNotEquals(
          "Expected definitions with different typeSets to be unequal", def1, def2);
    }

    @Test
    public void testSortEqual() {
      var sort1 =
          SortSpecBuilder.builder()
              .sortField(
                  SortFieldBuilder.builder()
                      .path("a")
                      .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                      .build())
              .buildSort();
      var sort2 =
          SortSpecBuilder.builder()
              .sortField(
                  SortFieldBuilder.builder()
                      .path("b")
                      .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                      .build())
              .buildSort();
      var sort3 =
          SortSpecBuilder.builder()
              .sortField(
                  SortFieldBuilder.builder()
                      .path("a")
                      .sortOption(
                          new UserFieldSortOptionsBuilder()
                              .sortOrder(SortOrder.ASC)
                              .nullEmptySortPosition(NullEmptySortPosition.HIGHEST)
                              .build())
                      .build())
              .buildSort();

      var def1 = BASIC_DEFINITION_BUILDER.sort(sort1).build();
      var def2 = BASIC_DEFINITION_BUILDER.sort(sort2).build();
      var def3 = BASIC_DEFINITION_BUILDER.sort(sort3).build();

      Assert.assertNotEquals(def1, def2);
      Assert.assertNotEquals(def1, def3);
    }

    @Test
    public void testDefaultFallbackNormalizerAndAnalyzer() {
      Assert.assertEquals(
          StockNormalizerName.NONE, SearchIndexDefinition.DEFAULT_FALLBACK_NORMALIZER);
      Assert.assertEquals(
          SearchIndexDefinition.DEFAULT_FALLBACK_ANALYZER,
          StockAnalyzerNames.LUCENE_STANDARD.getName());
    }

    @Test
    public void testFromBsonAllowsExtraFields() throws Exception {
      SearchIndexDefinition definition =
          SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();

      BsonDocument bson = definition.toBson();
      bson.put("lastUpdatedDate", new BsonDouble(new Date().getTime()));

      // Shouldn't throw.
      SearchIndexDefinition.fromBson(bson);
    }

    @Test
    public void testGetIndexId() {
      SearchIndexDefinition definition = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals(BASIC_INDEX_ID, definition.getIndexId());
    }

    @Test
    public void testGetName() {
      SearchIndexDefinition definition = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals("name", definition.getName());
    }

    @Test
    public void testGetDatabase() {
      SearchIndexDefinition definition = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals("database", definition.getDatabase());
    }

    @Test
    public void testGetLastObservedCollectionName() {
      SearchIndexDefinition definition = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals("collection", definition.getLastObservedCollectionName());
    }

    @Test
    public void testGetLastObservedNamespace() {
      SearchIndexDefinition definition = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals(
          new MongoNamespace("database.collection"), definition.getLastObservedNamespace());
    }

    @Test
    public void testSetLastObservedCollectionName() {
      SearchIndexDefinition definition = BASIC_DEFINITION_BUILDER.build();
      definition.setLastObservedCollectionName("new-collection");
      Assert.assertEquals("new-collection", definition.getLastObservedCollectionName());
      Assert.assertEquals(
          new MongoNamespace("database.new-collection"), definition.getLastObservedNamespace());
    }

    @Test
    public void testGetCollectionUuid() {
      SearchIndexDefinition definition = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals(BASIC_COLLECTION_UUID, definition.getCollectionUuid());
    }

    @Test
    public void testGetMappings() {
      SearchIndexDefinition definition = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals(BASIC_MAPPINGS, definition.getMappings());
    }

    @Test
    public void testGetAnalyzerName() {
      SearchIndexDefinition definitionWithoutAnalyzer = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals(Optional.empty(), definitionWithoutAnalyzer.getAnalyzerName());

      SearchIndexDefinition definitionWithAnalyzer =
          BASIC_DEFINITION_BUILDER.analyzerName("my-analyzer").build();
      Assert.assertEquals(Optional.of("my-analyzer"), definitionWithAnalyzer.getAnalyzerName());
    }

    @Test
    public void testGetSearchAnalyzerName() {
      SearchIndexDefinition definitionWithoutAnalyzer = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals(Optional.empty(), definitionWithoutAnalyzer.getSearchAnalyzerName());

      SearchIndexDefinition definitionWithAnalyzer =
          BASIC_DEFINITION_BUILDER.searchAnalyzerName("my-search-analyzer").build();
      Assert.assertEquals(
          Optional.of("my-search-analyzer"), definitionWithAnalyzer.getSearchAnalyzerName());
    }

    @Test
    public void testToString() {
      SearchIndexDefinition definition = BASIC_DEFINITION_BUILDER.build();
      Assert.assertEquals(
          "507f191e810c19729de860ea (index name collection collection "
              + "(eb6c40ca-f25e-47e8-b48c-02a05b64a5aa) in database database)",
          definition.toString());
    }

    @Test
    public void testGetStaticFields() {
      FieldDefinition aa = FieldDefinitionBuilder.builder().build();

      FieldDefinition a =
          FieldDefinitionBuilder.builder()
              .document(DocumentFieldDefinitionBuilder.builder().field("aa", aa).build())
              .build();

      FieldDefinition b =
          FieldDefinitionBuilder.builder()
              .string(
                  StringFieldDefinitionBuilder.builder()
                      .multi("multi", StringFieldDefinitionBuilder.builder().build())
                      .build())
              .build();

      FieldDefinition c = FieldDefinitionBuilder.builder().build();
      FieldDefinition d = FieldDefinitionBuilder.builder().build();

      SearchIndexDefinition definition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .field("a", a)
                      .field("b", b)
                      .field("c", c)
                      .field("d", d)
                      .build())
              .build();

      Set<Map.Entry<String, FieldDefinition>> expected =
          Set.of(
              Map.entry("a", a),
              Map.entry("a.aa", aa),
              Map.entry("b", b),
              Map.entry("c", c),
              Map.entry("d", d));
      Set<Map.Entry<String, FieldDefinition>> result = definition.getStaticFields().entrySet();

      Assert.assertEquals(expected, result);
    }

    @Test
    public void testGetStaticFieldsOfType() {
      FieldDefinition a =
          FieldDefinitionBuilder.builder()
              .date(DateFieldDefinitionBuilder.builder().build())
              .geo(GeoFieldDefinitionBuilder.builder().build())
              .build();
      FieldDefinition b =
          FieldDefinitionBuilder.builder()
              .geo(GeoFieldDefinitionBuilder.builder().build())
              .autocomplete(AutocompleteFieldDefinitionBuilder.builder().build())
              .document(DocumentFieldDefinitionBuilder.builder().build())
              .build();
      FieldDefinition c =
          FieldDefinitionBuilder.builder()
              .document(DocumentFieldDefinitionBuilder.builder().build())
              .bool(BooleanFieldDefinitionBuilder.builder().build())
              .build();
      SearchIndexDefinition definition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .field("a", a)
                      .field("b", b)
                      .field("c", c)
                      .build())
              .build();

      Assert.assertEquals(
          Set.of("a"),
          definition.getStaticFieldDefinitionsOfType(FieldDefinition::dateFieldDefinition).stream()
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet()));
      Assert.assertEquals(
          Set.of("a", "b"),
          definition.getStaticFieldDefinitionsOfType(FieldDefinition::geoFieldDefinition).stream()
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet()));
      Assert.assertEquals(
          Set.of("b"),
          definition
              .getStaticFieldDefinitionsOfType(FieldDefinition::autocompleteFieldDefinition)
              .stream()
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet()));
      Assert.assertEquals(
          Set.of("b", "c"),
          definition
              .getStaticFieldDefinitionsOfType(FieldDefinition::documentFieldDefinition)
              .stream()
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet()));
      Assert.assertEquals(
          Set.of("c"),
          definition
              .getStaticFieldDefinitionsOfType(FieldDefinition::booleanFieldDefinition)
              .stream()
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet()));

      Assert.assertTrue(
          definition
              .getStaticFieldDefinitionsOfType(FieldDefinition::stringFacetFieldDefinition)
              .isEmpty());
      Assert.assertTrue(
          definition
              .getStaticFieldDefinitionsOfType(FieldDefinition::objectIdFieldDefinition)
              .isEmpty());
      Assert.assertTrue(
          definition
              .getStaticFieldDefinitionsOfType(FieldDefinition::stringFieldDefinition)
              .isEmpty());
    }

    @Test
    public void testGetOverriddenAnalyzerNamesIgnoresStockAnalyzers() {
      var definition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "a",
                          FieldDefinitionBuilder.builder()
                              .string(
                                  StringFieldDefinitionBuilder.builder()
                                      .analyzerName("lucene.cjk")
                                      .build())
                              .build())
                      .build())
              .analyzerName("lucene.standard")
              .searchAnalyzerName("lucene.english")
              .build();
      Assert.assertEquals(Collections.emptySet(), definition.getNonStockAnalyzerNames());
    }

    @Test
    public void testGetAnalyzerNames() {
      SearchIndexDefinition definition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .analyzerName("index-analyzer")
              .searchAnalyzerName("index-search-analyzer")
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "first-level",
                          FieldDefinitionBuilder.builder()
                              .string(
                                  StringFieldDefinitionBuilder.builder()
                                      .analyzerName("first-level-analyzer")
                                      .searchAnalyzerName("first-level-search-analyzer")
                                      .multi(
                                          "multi",
                                          StringFieldDefinitionBuilder.builder()
                                              .analyzerName("first-level-multi-analyzer")
                                              .searchAnalyzerName(
                                                  "first-level-multi-search-analyzer")
                                              .build())
                                      .build())
                              .document(
                                  DocumentFieldDefinitionBuilder.builder()
                                      .field(
                                          "second-level",
                                          FieldDefinitionBuilder.builder()
                                              .string(
                                                  StringFieldDefinitionBuilder.builder()
                                                      .analyzerName("second-level-analyzer")
                                                      .searchAnalyzerName(
                                                          "second-level-search-analyzer")
                                                      .build())
                                              .build())
                                      .build())
                              .build())
                      .build())
              .build();

      Set<String> expected =
          Set.of(
              "index-analyzer",
              "index-search-analyzer",
              "first-level-analyzer",
              "first-level-search-analyzer",
              "first-level-multi-analyzer",
              "first-level-multi-search-analyzer",
              "second-level-analyzer",
              "second-level-search-analyzer");

      Assert.assertEquals(expected, definition.getNonStockAnalyzerNames());
    }

    @Test
    public void testIsFacetsEnabled() {

      var indexDefinitionWithFacetField =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "f",
                          FieldDefinitionBuilder.builder()
                              .stringFacet(StringFacetFieldDefinitionBuilder.builder().build())
                              .build())
                      .build())
              .build();

      Assert.assertTrue(indexDefinitionWithFacetField.isStringFacetsFieldIndexed());

      var indexDefinitionWithNoFacetField =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "f",
                          FieldDefinitionBuilder.builder()
                              .string(StringFieldDefinitionBuilder.builder().build())
                              .build())
                      .build())
              .build();

      Assert.assertFalse(indexDefinitionWithNoFacetField.isStringFacetsFieldIndexed());
    }

    @Test
    public void testDefaultStoredValue() {

      var indexDefinition =
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .storedSource(StoredSourceDefinition.defaultValue())
              .build();

      Assert.assertTrue(indexDefinition.getStoredSource().isAllExcluded());
    }

    @Test
    public void testIndexIdAtCreationTime_whenSet_returnsValue() {
      var indexId = new ObjectId("507f191e810c19729de860ea");
      var indexIdAtCreationTime = new ObjectId("607f191e810c19729de860eb");

      var indexDefinition =
          SearchIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .indexIdAtCreationTime(indexIdAtCreationTime)
              .build();

      Assert.assertTrue(indexDefinition.getIndexIdAtCreationTime().isPresent());
      Assert.assertEquals(indexIdAtCreationTime, indexDefinition.getIndexIdAtCreationTime().get());
    }

    @Test
    public void testIndexIdAtCreationTime_whenNotSet_returnsEmpty() {
      var indexDefinition =
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build();

      Assert.assertFalse(indexDefinition.getIndexIdAtCreationTime().isPresent());
    }

    @Test
    public void testIndexIdAtCreationTime_sameAsIndexId() {
      var indexId = new ObjectId("507f191e810c19729de860ea");

      var indexDefinition =
          SearchIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .indexIdAtCreationTime(indexId)
              .build();

      Assert.assertTrue(indexDefinition.getIndexIdAtCreationTime().isPresent());
      Assert.assertEquals(indexId, indexDefinition.getIndexIdAtCreationTime().get());
      Assert.assertEquals(indexId, indexDefinition.getIndexId());
    }

    @Test
    public void testIndexIdAtCreationTime_differentValue_affectsEquality() {
      var indexId = new ObjectId("507f191e810c19729de860ea");
      var indexIdAtCreationTime = new ObjectId("607f191e810c19729de860eb");

      var definitionWithCreationTime =
          SearchIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .indexIdAtCreationTime(indexIdAtCreationTime)
              .build();

      var definitionWithoutCreationTime =
          SearchIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build();

      // Different because indexIdAtCreationTime (607f) != indexId (507f)
      Assert.assertNotEquals(definitionWithCreationTime, definitionWithoutCreationTime);
      Assert.assertNotEquals(
          definitionWithCreationTime.hashCode(), definitionWithoutCreationTime.hashCode());
    }

    @Test
    public void testIndexIdAtCreationTime_sameAsIndexId_doesNotAffectEquality() {
      var indexId = new ObjectId("507f191e810c19729de860ea");

      var definitionWithCreationTimeSameAsIndexId =
          SearchIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .indexIdAtCreationTime(indexId) // same as indexId
              .build();

      var definitionWithoutCreationTime =
          SearchIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build();

      // Equal because indexIdAtCreationTime.orElse(indexId) == indexId for both
      Assert.assertEquals(definitionWithCreationTimeSameAsIndexId, definitionWithoutCreationTime);
      Assert.assertEquals(
          definitionWithCreationTimeSameAsIndexId.hashCode(),
          definitionWithoutCreationTime.hashCode());
    }

    @Test
    public void testIndexIdAtCreationTime_roundTrip() throws Exception {
      var indexIdAtCreationTime = new ObjectId("607f191e810c19729de860eb");

      VectorIndexDefinition original =
          VectorIndexDefinitionBuilder.builder()
              .indexIdAtCreationTime(indexIdAtCreationTime)
              .withCosineVectorField("my.vector.field", 128)
              .build();

      BsonDocument bson = original.toBson();
      VectorIndexDefinition parsed = VectorIndexDefinition.fromBson(bson);

      Truth.assertThat(parsed.getIndexIdAtCreationTime()).isPresent();
      Truth.assertThat(parsed.getIndexIdAtCreationTime().get()).isEqualTo(indexIdAtCreationTime);
      assertEquals(original, parsed);
    }

    @Test
    public void testDefinitionVersionId_roundTrip() throws Exception {
      var definitionVersionId = new ObjectId("707f191e810c19729de860ec");
      SearchIndexDefinition original =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .dynamicMapping()
              .definitionVersionId(definitionVersionId)
              .build();

      SearchIndexDefinition parsed = SearchIndexDefinition.fromBson(original.toBson());

      Truth.assertThat(parsed.getDefinitionVersionId()).hasValue(definitionVersionId);
      assertEquals(original, parsed);
    }

    @Test
    public void testDefinitionVersionId_whenNotSet_omittedFromBson() {
      SearchIndexDefinition definition =
          SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();

      Truth.assertThat(definition.getDefinitionVersionId()).isEmpty();
      Truth.assertThat(definition.toBson().containsKey("definitionVersionId")).isFalse();
    }

    @Test
    public void testDefinitionVersionId_differentValue_affectsEquality() {
      SearchIndexDefinition base =
          SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();
      SearchIndexDefinition first =
          SearchIndexDefinitionBuilder.from(base)
              .definitionVersionId(new ObjectId("707f191e810c19729de860ec"))
              .build();
      SearchIndexDefinition second =
          SearchIndexDefinitionBuilder.from(base)
              .definitionVersionId(new ObjectId("707f191e810c19729de860ed"))
              .build();

      Assert.assertNotEquals(first, second);
      Assert.assertTrue(first.equalsIgnoringDefinitionVersion(second));
    }
  }
}
