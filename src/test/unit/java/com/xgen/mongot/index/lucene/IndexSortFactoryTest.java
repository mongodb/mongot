package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.InstrumentedConcurrentMergeSchedulerTest.createMergeScheduler;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.ingestion.BsonDocumentProcessor;
import com.xgen.mongot.index.lucene.document.DefaultIndexingPolicy;
import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldName.MetaField;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.query.LuceneSearchQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.query.sort.LuceneSortFactory;
import com.xgen.mongot.index.lucene.query.sort.mixed.MqlMixedSort;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.sort.LuceneIndexSortFactory;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.BooleanFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.ObjectIdFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.UuidFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneIndexUtils;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class IndexSortFactoryTest {

  // Validate the returned index sort is the same as the one created by LuceneSortFactory
  @RunWith(Parameterized.class)
  public static class ValidateQuerySortFieldTest {
    public record TestSpec(
        FieldName.TypeField typeField, Consumer<SearchFieldDefinitionResolver> mockResolverSetup) {}

    private final TestSpec testSpec;
    private SearchFieldDefinitionResolver mockResolver;

    public ValidateQuerySortFieldTest(TestSpec testSpec) {
      this.testSpec = testSpec;
    }

    @Before
    public void setUp() {
      this.mockResolver = mock(SearchFieldDefinitionResolver.class);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestSpec> data() {
      return List.of(
          new TestSpec(
              FieldName.TypeField.TOKEN,
              mockResolver -> {
                when(mockResolver.getFieldDefinition(any(), any()))
                    .thenReturn(
                        Optional.of(
                            FieldDefinitionBuilder.builder()
                                .token(TokenFieldDefinitionBuilder.builder().build())
                                .build()));
              }),
          new TestSpec(
              FieldName.TypeField.BOOLEAN,
              mockResolver -> {
                when(mockResolver.getFieldDefinition(any(), any()))
                    .thenReturn(
                        Optional.of(
                            FieldDefinitionBuilder.builder()
                                .bool(BooleanFieldDefinitionBuilder.builder().build())
                                .build()));
              }),
          new TestSpec(
              FieldName.TypeField.DATE_V2,
              mockResolver -> {
                when(mockResolver.getFieldDefinition(any(), any()))
                    .thenReturn(
                        Optional.of(
                            FieldDefinitionBuilder.builder()
                                .date(DateFieldDefinitionBuilder.builder().build())
                                .build()));
              }),
          new TestSpec(
              TypeField.NUMBER_INT64_V2,
              mockResolver -> {
                when(mockResolver.getFieldDefinition(any(), any()))
                    .thenReturn(
                        Optional.of(
                            FieldDefinitionBuilder.builder()
                                .number(
                                    NumericFieldDefinitionBuilder.builder()
                                        .indexIntegers(true)
                                        .representation(NumericFieldOptions.Representation.INT64)
                                        .buildNumberField())
                                .build()));
              }),
          new TestSpec(
              TypeField.NUMBER_DOUBLE_V2,
              mockResolver -> {
                when(mockResolver.getFieldDefinition(any(), any()))
                    .thenReturn(
                        Optional.of(
                            FieldDefinitionBuilder.builder()
                                .number(
                                    NumericFieldDefinitionBuilder.builder()
                                        .indexDoubles(true)
                                        .representation(NumericFieldOptions.Representation.DOUBLE)
                                        .buildNumberField())
                                .build()));
              }),
          new TestSpec(
              TypeField.UUID,
              mockResolver -> {
                when(mockResolver.getFieldDefinition(any(), any()))
                    .thenReturn(
                        Optional.of(
                            FieldDefinitionBuilder.builder()
                                .uuid(UuidFieldDefinitionBuilder.builder().build())
                                .build()));
              }),
          new TestSpec(
              TypeField.OBJECT_ID,
              mockResolver -> {
                when(mockResolver.getFieldDefinition(any(), any()))
                    .thenReturn(
                        Optional.of(
                            FieldDefinitionBuilder.builder()
                                .objectid(ObjectIdFieldDefinitionBuilder.builder().build())
                                .build()));
              }));
    }

    @Test
    public void runTest() {
      // Setup mocks
      when(this.mockResolver.getIndexCapabilities()).thenReturn(SearchIndexCapabilities.CURRENT);
      this.testSpec.mockResolverSetup.accept(this.mockResolver);
      var sortFactory = new LuceneIndexSortFactory(this.mockResolver);

      Sort sortSpec =
          new Sort(
              ImmutableList.of(
                  new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_DESC)));
      org.apache.lucene.search.Sort actualSort = sortFactory.createIndexSort(sortSpec);

      List<SortField> expectedSortFields =
          sortSpec.getSortFields().stream()
              .map(
                  mongotSortField ->
                      LuceneSortFactory.createOptimizedSortField(
                              mongotSortField,
                              ImmutableSet.of(this.testSpec.typeField),
                              Optional.empty(),
                              Optional.empty(),
                              this.mockResolver.getIndexCapabilities(),
                              true)
                          .get())
              .toList();
      // For INT64_V2 and DATE_V2, createIndexSort prepends a nullness sort field.
      SortField[] actualSortFields = actualSort.getSort();
      @Var int nullnessOffset = 0;
      if (this.testSpec.typeField == TypeField.NUMBER_INT64_V2
          || this.testSpec.typeField == TypeField.DATE_V2) {
        nullnessOffset = 1;
        Assert.assertTrue(
            actualSortFields[0].getField().startsWith(
                FieldName.MetaField.NULLNESS.getLuceneFieldName()));
      }

      Assert.assertEquals(expectedSortFields.size() + nullnessOffset, actualSortFields.length);
      for (int i = 0; i < expectedSortFields.size(); i++) {
        var expected = expectedSortFields.get(i);
        var actual = actualSortFields[i + nullnessOffset];
        Assert.assertEquals(expected, actual);
      }
    }
  }

  // Validate that the lucene sort spec fields are actually existent in the document.
  @RunWith(Parameterized.class)
  public static class ValidateLuceneFieldsExistTest {
    public record TestSpec(FieldDefinition filedDefinition, BsonValue value) {}

    private final TestSpec testSpec;

    public ValidateLuceneFieldsExistTest(TestSpec testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestSpec> data() {
      return List.of(
          new TestSpec(
              FieldDefinitionBuilder.builder()
                  .token(TokenFieldDefinitionBuilder.builder().build())
                  .build(),
              new BsonString("value")),
          new TestSpec(
              FieldDefinitionBuilder.builder()
                  .bool(BooleanFieldDefinitionBuilder.builder().build())
                  .build(),
              BsonBoolean.TRUE),
          new TestSpec(
              FieldDefinitionBuilder.builder()
                  .date(DateFieldDefinitionBuilder.builder().build())
                  .build(),
              new BsonDateTime(123456789)),
          new TestSpec(
              FieldDefinitionBuilder.builder()
                  .number(
                      NumericFieldDefinitionBuilder.builder()
                          .indexDoubles(true)
                          .representation(NumericFieldOptions.Representation.DOUBLE)
                          .buildNumberField())
                  .build(),
              new BsonDouble(1.0)),
          new TestSpec(
              FieldDefinitionBuilder.builder()
                  .number(
                      NumericFieldDefinitionBuilder.builder()
                          .indexDoubles(true)
                          .representation(NumericFieldOptions.Representation.INT64)
                          .buildNumberField())
                  .build(),
              new BsonDouble(1.0)),
          new TestSpec(
              FieldDefinitionBuilder.builder()
                  .number(
                      NumericFieldDefinitionBuilder.builder()
                          .indexIntegers(true)
                          .representation(NumericFieldOptions.Representation.INT64)
                          .buildNumberField())
                  .build(),
              new BsonInt64(1)),
          new TestSpec(
              FieldDefinitionBuilder.builder()
                  .number(
                      NumericFieldDefinitionBuilder.builder()
                          .indexIntegers(true)
                          .representation(NumericFieldOptions.Representation.DOUBLE)
                          .buildNumberField())
                  .build(),
              new BsonInt64(1)),
          new TestSpec(
              FieldDefinitionBuilder.builder()
                  .uuid(UuidFieldDefinitionBuilder.builder().build())
                  .build(),
              new BsonBinary(new UUID(1, 2))),
          new TestSpec(
              FieldDefinitionBuilder.builder()
                  .objectid(ObjectIdFieldDefinitionBuilder.builder().build())
                  .build(),
              new BsonObjectId(new ObjectId())));
    }

    @Test
    public void runTest() throws IOException {
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .field("f", this.testSpec.filedDefinition)
              .build();

      Sort sortSpec =
          new Sort(
              ImmutableList.of(
                  new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_DESC)));

      SearchIndexDefinition indexDefinition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(mappings)
              .sort(sortSpec)
              .build();

      BsonDocument bson =
          new BsonDocument()
              .append("_id", new BsonObjectId(new ObjectId()))
              .append("f", this.testSpec.value);
      SearchFieldDefinitionResolver resolver =
          indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT);
      DocumentBuilder builder =
          DefaultIndexingPolicy.RootDocumentIndexingPolicy.create(
                  AnalyzerRegistryBuilder.empty().getNormalizer(StockNormalizerName.NONE),
                  resolver,
                  new IndexMetricsUpdater.IndexingMetricsUpdater(
                      SearchIndex.mockMetricsFactory(), indexDefinition.getType()))
              .createBuilder(LuceneIndexUtils.encodeDocumentId(bson.get("_id")));
      BsonDocumentProcessor.process(BsonUtils.documentToRaw(bson), builder);
      Document document = builder.build();

      LuceneIndexSortFactory sortFactory = new LuceneIndexSortFactory(resolver);
      org.apache.lucene.search.Sort luceneSort = sortFactory.createIndexSort(sortSpec);

      for (SortField sortField : luceneSort.getSort()) {
        Assert.assertNotNull(document.getField(sortField.getField()));
      }
    }
  }

  // End-to-end tests from IndexWriter to validate lucene documents are sorted correctly.
  @RunWith(Parameterized.class)
  public static class ValidateLuceneSortTest {
    @FunctionalInterface
    private interface SortValuesExtractor {
      List<Object> extractSortValues(LeafReader reader, SortSelector selector, String fieldName)
          throws IOException;
    }

    public record TestSpec(
        FieldDefinition filedDefinition,
        UserFieldSortOptions options,
        SortValuesExtractor sortValuesExtractor,
        Comparator<Object> comparator,
        List<BsonValue> values,
        Optional<String> sortFieldName,
        Optional<Long> expectedNullCount) {

      public TestSpec(
          FieldDefinition filedDefinition,
          UserFieldSortOptions options,
          SortValuesExtractor sortValuesExtractor,
          Comparator<Object> comparator,
          List<BsonValue> values) {
        this(
            filedDefinition,
            options,
            sortValuesExtractor,
            comparator,
            values,
            Optional.empty(),
            Optional.empty());
      }
    }

    private final TestSpec testSpec;
    private Directory directory;
    private SingleLuceneIndexWriter indexWriter;
    private SearchFieldDefinitionResolver resolver;

    public ValidateLuceneSortTest(TestSpec testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestSpec> data() {
      return List.of(
          testInt64(),
          testDouble(),
          testDate(),
          testToken(),
          testBoolean(),
          testUuid(),
          testObjectId(),
          testTokenNullFirst(),
          testTokenNullLast(),
          testLongNullLast(),
          testDateNullLast(),
          testTokenMinSelector(),
          testTokenMaxSelector(),
          testLongMinSelector(),
          testLongMaxSelector(),
          testEmbeddedToken());
    }

    @Before
    public void setUp() throws Exception {
      this.directory = new ByteBuffersDirectory();
      DocumentFieldDefinition mappings =
          DocumentFieldDefinitionBuilder.builder()
              .field("f", this.testSpec.filedDefinition)
              .build();
      Sort sort =
          new Sort(
              ImmutableList.of(
                  new MongotSortField(
                      this.testSpec
                          .sortFieldName
                          .map(FieldPath::parse)
                          .orElse(FieldPath.newRoot("f")),
                      this.testSpec.options)));
      var indexDefinition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(mappings)
              .sort(sort)
              .build();

      this.resolver = indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT);

      var mergeScheduler =
          createMergeScheduler(
              new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
              MOCK_INDEX_GENERATION_ID,
              0,
              1,
              false, 
              this.directory);
      mergeScheduler.getIn().setMaxMergesAndThreads(10, 4);

      this.indexWriter =
          SingleLuceneIndexWriter.createForSearchIndex(
              this.directory,
              mergeScheduler,
              new TieredMergePolicy(),
              100,
              Optional.empty(), // field limit
              Optional.empty(), // docs limit
              LuceneAnalyzer.indexAnalyzer(indexDefinition, AnalyzerRegistryBuilder.empty()),
              this.resolver,
              SearchIndex.mockIndexingMetricsUpdater(indexDefinition.getType()),
              Optional.empty(),
              FeatureFlags.withDefaults().enable(Feature.SORTED_INDEX).build(),
              DynamicFeatureFlagRegistry.empty(),
              false);

      // Configure IndexWriter to have multiple segments by forcing a flush every 2 documents.
      this.indexWriter.getLuceneWriter().getConfig().setMaxBufferedDocs(2);
    }

    @After
    public void tearDown() throws Exception {
      if (this.indexWriter != null) {
        this.indexWriter.close();
      }
      if (this.directory != null) {
        this.directory.close();
      }
    }

    @Test
    public void runTest() throws Exception {
      indexDocuments(this.testSpec.values);
      this.indexWriter.commit(EncodedUserData.EMPTY);

      LuceneIndexSortFactory sortFactory = new LuceneIndexSortFactory(this.resolver);
      Sort sortSpec =
          new Sort(
              ImmutableList.of(
                  new MongotSortField(
                      this.testSpec
                          .sortFieldName
                          .map(FieldPath::parse)
                          .orElse(FieldPath.newRoot("f")),
                      this.testSpec.options)));
      org.apache.lucene.search.Sort luceneSort = sortFactory.createIndexSort(sortSpec);

      @Var int totalDocs = 0;
      try (DirectoryReader reader = DirectoryReader.open(this.directory)) {
        Assert.assertTrue(
            "Expect multiple segments, got " + reader.leaves().size(), reader.leaves().size() > 1);
        for (var leafReaderContext : reader.leaves()) {
          totalDocs += validateSegmentSortOrder(leafReaderContext.reader(), luceneSort, false);
        }
      }
      Assert.assertEquals(this.testSpec.values.size(), totalDocs);

      // validate merged segments respect the sort orders.
      this.indexWriter.getLuceneWriter().getConfig().setMaxBufferedDocs(10000);
      this.indexWriter.getLuceneWriter().forceMerge(1);
      this.indexWriter.commit(EncodedUserData.EMPTY);
      totalDocs = 0;
      try (DirectoryReader reader = DirectoryReader.open(this.directory)) {
        Assert.assertEquals(1, reader.leaves().size());
        for (var leafReaderContext : reader.leaves()) {
          totalDocs += validateSegmentSortOrder(leafReaderContext.reader(), luceneSort, true);
        }
      }
      Assert.assertEquals(this.testSpec.values.size(), totalDocs);
    }

    private void indexDocuments(List<BsonValue> values) throws Exception {
      @Var int counter = 0;
      for (BsonValue value : values) {
        BsonDocument bson =
            new BsonDocument()
                // make sure each document has a unique _id
                .append("_id", new BsonObjectId(new ObjectId(Date.from(Instant.now()), counter)))
                .append("f", value);

        BsonDocument bsonWithMetadata =
            bson.clone().append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", bson.get("_id")));

        this.indexWriter.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(BsonUtils.documentToRaw(bsonWithMetadata)), MOCK_INDEX_ID),
                BsonUtils.documentToRaw(bsonWithMetadata)));
        counter++;
      }
    }

    // return number of documents in the segment.
    private int validateSegmentSortOrder(
        LeafReader leafReader, org.apache.lucene.search.Sort luceneSort, boolean isSingleSegment)
        throws IOException {
      List<IndexSorter.DocComparator> docComparators = new ArrayList<>();

      // Check if we need parent-aware comparison for embedded documents
      NumericDocValues parentDocValues =
          leafReader.getNumericDocValues(FieldName.MetaField.PARENT_FIELD.getLuceneFieldName());
      BitSet parents =
          parentDocValues != null ? BitSet.of(parentDocValues, leafReader.maxDoc()) : null;

      for (SortField sortField : luceneSort.getSort()) {
        var indexSorter = sortField.getIndexSorter();
        var baseComparator = indexSorter.getDocComparator(leafReader, leafReader.maxDoc());
        if (parents != null) {
          docComparators.add(
              (int docId1, int docId2) -> {
                int parentDoc1 = parents.nextSetBit(docId1);
                Assert.assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, parentDoc1);
                int parentDoc2 = parents.nextSetBit(docId2);
                Assert.assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, parentDoc2);
                return baseComparator.compare(parentDoc1, parentDoc2);
              });
        } else {
          docComparators.add(baseComparator);
        }
      }

      IndexSorter.DocComparator compositeDocComparator =
          (int docId1, int docId2) -> {
            for (var docComparator : docComparators) {
              int cmp = docComparator.compare(docId1, docId2);
              if (cmp != 0) {
                return cmp;
              }
            }
            return 0;
          };

      List<Integer> docIds =
          IntStream.range(0, leafReader.maxDoc())
              .filter(
                  docId -> leafReader.getLiveDocs() == null || leafReader.getLiveDocs().get(docId))
              .boxed()
              .toList();

      // This validates documents are actually sorted based on Sort definition, however
      // it doesn't validate the Sort specification is correct.
      boolean isSorted =
          IntStream.range(0, docIds.size() - 1)
              .allMatch(i -> compositeDocComparator.compare(docIds.get(i), docIds.get(i + 1)) <= 0);
      Assert.assertTrue(
          "Documents in segment are not sortd for type: " + this.testSpec.filedDefinition,
          isSorted);

      // Validate actual values to make sure the sort spec is correct as in TestSpec
      // To simplify the test, we only validate the first sort field.
      List<Object> sortValues =
          this.testSpec.sortValuesExtractor.extractSortValues(
              leafReader, this.testSpec.options.selector(), luceneSort.getSort()[0].getField());

      validateActualSortValues(sortValues);
      validateNullPositioning(sortValues, isSingleSegment);
      validateEmbeddedDocumentBlocks(leafReader, isSingleSegment);

      return sortValues.size();
    }

    private void validateActualSortValues(List<Object> sortValues) {
      for (int i = 0; i < sortValues.size() - 1; i++) {
        var current = sortValues.get(i);
        var next = sortValues.get(i + 1);

        int comparison = this.testSpec.comparator.compare(current, next);
        boolean nullsAsFront =
            isNullsAsFront(
                this.testSpec.options.nullEmptySortPosition(), this.testSpec.options.order());

        // null values handling
        if (current == null && next == null) {
          Assert.assertEquals(0, comparison);
          continue;
        } else if (current == null) {
          if (nullsAsFront) {
            Assert.assertTrue(comparison < 0);
          } else {
            Assert.assertTrue(comparison > 0);
          }
          continue;
        } else if (next == null) {
          if (nullsAsFront) {
            Assert.assertTrue(comparison > 0);
          } else {
            Assert.assertTrue(comparison < 0);
          }
          continue;
        }

        if (this.testSpec.options.order() == SortOrder.ASC) {
          Assert.assertTrue(
              String.format(
                  "Documents not in ascending order at position %d: %s > %s", i, current, next),
              comparison <= 0);
        } else {
          Assert.assertTrue(
              String.format(
                  "Documents not in descending order at position %d: %s < %s", i, current, next),
              comparison >= 0);
        }
      }
    }

    private void validateNullPositioning(List<Object> sortValues, boolean validateNullCount) {
      long extractedNullCount = sortValues.stream().filter(Objects::isNull).count();

      if (validateNullCount && this.testSpec.expectedNullCount.isPresent()) {
        Assert.assertEquals(this.testSpec.expectedNullCount.get().longValue(), extractedNullCount);
      }

      if (extractedNullCount == 0) {
        return;
      }

      var nullPosition = this.testSpec.options.nullEmptySortPosition();
      if (isNullsAsFront(nullPosition, this.testSpec.options.order())) {
        for (int i = 0; i < extractedNullCount; i++) {
          Assert.assertNull(sortValues.get(i));
        }
      } else {
        for (int i = sortValues.size() - (int) extractedNullCount; i < sortValues.size(); i++) {
          Assert.assertNull(sortValues.get(i));
        }
      }
    }

    private void validateEmbeddedDocumentBlocks(LeafReader leafReader, boolean isSingleSegment)
        throws IOException {
      // Build BitSet of parent documents using EMBEDDED_ROOT field
      Terms embeddedRootTerms =
          leafReader.terms(FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName());
      if (embeddedRootTerms == null) {
        return;
      }

      TermsEnum termsEnum = embeddedRootTerms.iterator();
      Assert.assertTrue(termsEnum.seekExact(new BytesRef(FieldValue.EMBEDDED_ROOT_FIELD_VALUE)));
      PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
      BitSet parents = BitSet.of(postings, leafReader.maxDoc());
      if (isSingleSegment) {
        // In single segment, all parent documents should be indexed
        Assert.assertEquals(parents.cardinality(), this.testSpec.values.size());
      }

      // Build BitSet of child documents using EMBEDDED_PATH field
      Terms embeddedPathTerms =
          leafReader.terms(FieldName.MetaField.EMBEDDED_PATH.getLuceneFieldName());
      Assert.assertNotNull(embeddedPathTerms);
      TermsEnum pathTermsEnum = embeddedPathTerms.iterator();

      // Assume top field is embedded root
      Assert.assertTrue(
          this.testSpec.filedDefinition.embeddedDocumentsFieldDefinition().isPresent());
      String embeddedRootPath = "f";
      Assert.assertTrue(pathTermsEnum.seekExact(new BytesRef("f")));
      var childDocs =
          BitSet.of(pathTermsEnum.postings(null, PostingsEnum.NONE), leafReader.maxDoc());
      Assert.assertNotEquals(0, childDocs.cardinality());

      // Validate that all child documents are under parent documents by checking:
      // 1. there is always a parent doc after it
      // 2. both docs contain the same meta id field value
      for (int childDoc = 0; childDoc < leafReader.maxDoc(); childDoc++) {
        if (!childDocs.get(childDoc)) {
          continue;
        }
        int parentDoc = parents.nextSetBit(childDoc);
        Assert.assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, parentDoc);

        // get _id value through stored fields
        BytesRef parentId =
            leafReader
                .storedFields()
                .document(parentDoc, Set.of(MetaField.ID.getLuceneFieldName()))
                .getBinaryValue(FieldName.MetaField.ID.getLuceneFieldName());
        Assert.assertNotNull(parentId);
        BytesRef childId =
            leafReader
                .storedFields()
                .document(childDoc, Set.of(MetaField.ID.getLuceneFieldName()))
                .getBinaryValue(FieldName.MetaField.ID.getLuceneFieldName());

        Assert.assertEquals(
            String.format(
                "Child document %d and parent %d have different _id values for "
                    + "embedded root '%s'",
                childDoc, parentDoc, embeddedRootPath),
            parentId,
            childId);
      }
    }

    private static boolean isNullsAsFront(NullEmptySortPosition nullPosition, SortOrder sortOrder) {
      return ((nullPosition == NullEmptySortPosition.LOWEST) && (sortOrder == SortOrder.ASC))
          || ((nullPosition == NullEmptySortPosition.HIGHEST) && (sortOrder == SortOrder.DESC));
    }

    private static List<Object> extractNumberValues(
        LeafReader reader, SortSelector selector, String fieldName) throws IOException {
      // regardless of Double or Long, mongot always uses Long type.
      NumericDocValues docValues =
          SortedNumericSelector.wrap(
              DocValues.getSortedNumeric(reader, fieldName),
              selector.numericSelector,
              SortField.Type.LONG);
      List<Object> values = new ArrayList<>();

      for (int docId = 0; docId < reader.maxDoc(); docId++) {
        if (docValues.advanceExact(docId)) {
          values.add(docValues.longValue());
        } else {
          values.add(null);
        }
      }
      return values;
    }

    private static List<Object> extractDateValues(
        LeafReader reader, SortSelector selector, String fieldName) throws IOException {
      return extractNumberValues(reader, selector, fieldName);
    }

    private static List<Object> extractBytesRefValues(
        LeafReader reader,
        SortSelector selector,
        String fieldName,
        Function<BytesRef, Object> converter)
        throws IOException {
      SortedDocValues docValues =
          SortedSetSelector.wrap(
              DocValues.getSortedSet(reader, fieldName), selector.sortedSetSelector);
      List<Object> values = new ArrayList<>();

      for (int docId = 0; docId < reader.maxDoc(); docId++) {
        if (docValues.advanceExact(docId)) {
          values.add(converter.apply(docValues.lookupOrd(docValues.ordValue())));
        } else {
          values.add(null);
        }
      }
      return values;
    }

    private static List<Object> extractTokenValues(
        LeafReader reader, SortSelector selector, String fieldName) throws IOException {
      return extractBytesRefValues(
          reader, selector, fieldName, bytes -> new BsonString(bytes.utf8ToString()));
    }

    private static List<Object> extractBooleanValues(
        LeafReader reader, SortSelector selector, String fieldName) throws IOException {
      return extractBytesRefValues(
          reader,
          selector,
          fieldName,
          ref ->
              BsonBoolean.valueOf(
                  ref.bytesEquals(new BytesRef(FieldValue.BOOLEAN_TRUE_FIELD_VALUE))));
    }

    private static List<Object> extractUuidValues(
        LeafReader reader, SortSelector selector, String fieldName) throws IOException {
      return extractBytesRefValues(
          reader, selector, fieldName, ref -> UUID.fromString(ref.utf8ToString()));
    }

    private static List<Object> extractObjectIdValues(
        LeafReader reader, SortSelector selector, String fieldName) throws IOException {
      return extractBytesRefValues(
          reader,
          selector,
          fieldName,
          ref -> new ObjectId(ByteBuffer.wrap(ref.bytes, ref.offset, ref.length)));
    }

    private static List<Object> extractEmbeddedTokenValues(
        LeafReader reader, SortSelector selector, String fieldName) throws IOException {
      // Get parent field doc values to identify parent documents
      NumericDocValues parentDocValues =
          reader.getNumericDocValues(FieldName.MetaField.PARENT_FIELD.getLuceneFieldName());
      Assert.assertNotNull(parentDocValues);
      BitSet parents = BitSet.of(parentDocValues, reader.maxDoc());

      // Extract sort values using parent document lookup.
      SortedDocValues docValues =
          SortedSetSelector.wrap(
              DocValues.getSortedSet(reader, fieldName), selector.sortedSetSelector);
      List<Object> values = new ArrayList<>();

      // We only care about parent documents.
      for (int docId = 0; docId < reader.maxDoc(); docId++) {
        if (parents.get(docId)) {
          if (docValues.advanceExact(docId)) {
            values.add(new BsonString(docValues.lookupOrd(docValues.ordValue()).utf8ToString()));
          } else {
            values.add(null);
          }
        }
      }
      return values;
    }

    // Test case 1: int64
    private static TestSpec testInt64() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .number(
                  NumericFieldDefinitionBuilder.builder()
                      .indexIntegers(true)
                      .representation(NumericFieldOptions.Representation.INT64)
                      .buildNumberField())
              .build(),
          UserFieldSortOptions.DEFAULT_ASC,
          ValidateLuceneSortTest::extractNumberValues,
          (o1, o2) -> Long.compare((Long) o1, (Long) o2),
          List.of(new BsonInt64(100), new BsonInt64(99), new BsonInt64(98), new BsonInt64(97)));
    }

    // Test case 2: double
    private static TestSpec testDouble() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .number(
                  NumericFieldDefinitionBuilder.builder()
                      .indexDoubles(true)
                      .representation(NumericFieldOptions.Representation.DOUBLE)
                      .buildNumberField())
              .build(),
          UserFieldSortOptions.DEFAULT_ASC,
          ValidateLuceneSortTest::extractNumberValues,
          (o1, o2) -> {
            // convert to double first
            double d1 = LuceneDoubleConversionUtils.fromMqlSortableLong((Long) o1);
            double d2 = LuceneDoubleConversionUtils.fromMqlSortableLong((Long) o2);
            return Double.compare(d1, d2);
          },
          List.of(
              new BsonDouble(100.0),
              new BsonDouble(99.0),
              new BsonDouble(98.0),
              new BsonDouble(97.0)));
    }

    // Test case 3: date
    private static TestSpec testDate() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .date(DateFieldDefinitionBuilder.builder().build())
              .build(),
          UserFieldSortOptions.DEFAULT_ASC,
          ValidateLuceneSortTest::extractDateValues,
          (o1, o2) -> {
            BsonDateTime d1 = new BsonDateTime((Long) o1);
            BsonDateTime d2 = new BsonDateTime((Long) o2);
            return d1.compareTo(d2);
          },
          List.of(
              new BsonDateTime(100),
              new BsonDateTime(99),
              new BsonDateTime(98),
              new BsonDateTime(97)));
    }

    // Test case 4: token
    private static TestSpec testToken() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .token(TokenFieldDefinitionBuilder.builder().build())
              .build(),
          UserFieldSortOptions.DEFAULT_ASC,
          ValidateLuceneSortTest::extractTokenValues,
          (o1, o2) -> {
            BsonString s1 = (BsonString) o1;
            BsonString s2 = (BsonString) o2;
            return s1.compareTo(s2);
          },
          List.of(
              new BsonString("eef"),
              new BsonString("daf"),
              new BsonString("bbc"),
              new BsonString("abc")));
    }

    // Test case 5: boolean
    private static TestSpec testBoolean() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .bool(BooleanFieldDefinitionBuilder.builder().build())
              .build(),
          UserFieldSortOptions.DEFAULT_ASC,
          ValidateLuceneSortTest::extractBooleanValues,
          (o1, o2) -> {
            BsonBoolean b1 = (BsonBoolean) o1;
            BsonBoolean b2 = (BsonBoolean) o2;
            return b1.compareTo(b2);
          },
          List.of(BsonBoolean.TRUE, BsonBoolean.FALSE, BsonBoolean.TRUE, BsonBoolean.FALSE));
    }

    // Test case 6: uuid
    private static TestSpec testUuid() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .uuid(UuidFieldDefinitionBuilder.builder().build())
              .build(),
          UserFieldSortOptions.DEFAULT_ASC,
          ValidateLuceneSortTest::extractUuidValues,
          (o1, o2) -> {
            UUID u1 = (UUID) o1;
            UUID u2 = (UUID) o2;
            return u1.compareTo(u2);
          },
          List.of(
              new BsonBinary(new UUID(0x0000000000000003L, 0x0000000000000000L)),
              new BsonBinary(new UUID(0x0000000000000001L, 0x0000000000000000L)),
              new BsonBinary(new UUID(0x0000000000000004L, 0x0000000000000000L)),
              new BsonBinary(new UUID(0x0000000000000002L, 0x0000000000000000L))));
    }

    // Test case 7: objectid
    private static TestSpec testObjectId() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .objectid(ObjectIdFieldDefinitionBuilder.builder().build())
              .build(),
          UserFieldSortOptions.DEFAULT_ASC,
          ValidateLuceneSortTest::extractObjectIdValues,
          (o1, o2) -> {
            ObjectId oid1 = (ObjectId) o1;
            ObjectId oid2 = (ObjectId) o2;
            return oid1.compareTo(oid2);
          },
          List.of(
              new BsonObjectId(new ObjectId("507f1f77bcf86cd799439013")), // Higher timestamp
              new BsonObjectId(new ObjectId("507f1f77bcf86cd799439011")), // Lower timestamp
              new BsonObjectId(new ObjectId("507f1f77bcf86cd799439014")), // Highest timestamp
              new BsonObjectId(new ObjectId("507f1f77bcf86cd799439012")) // Middle timestamp
              ));
    }

    // Test case 8: token null first
    private static TestSpec testTokenNullFirst() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .document(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "subfield",
                          FieldDefinitionBuilder.builder()
                              .token(TokenFieldDefinitionBuilder.builder().build())
                              .build())
                      .build())
              .build(),
          new UserFieldSortOptions(SortOrder.ASC, SortSelector.MIN, NullEmptySortPosition.LOWEST),
          ValidateLuceneSortTest::extractTokenValues,
          (o1, o2) -> {
            if (o1 == null && o2 == null) {
              return 0;
            }
            if (o1 == null) {
              return -1;
            } // null first
            if (o2 == null) {
              return 1;
            }
            BsonString s1 = (BsonString) o1;
            BsonString s2 = (BsonString) o2;
            return s1.compareTo(s2);
          },
          List.of(
              new BsonDocument("subfield", new BsonString("zebra")), // has subfield
              new BsonDocument(), // missing subfield (null)
              new BsonDocument("subfield", new BsonString("apple")), // has subfield
              new BsonDocument(), // missing subfield (null)
              new BsonDocument(), // missing subfield (null)
              new BsonDocument("subfield", new BsonString("banana")) // has subfield
              ),
          Optional.of("f.subfield"),
          Optional.of(3L));
    }

    // Test case 9: token null last
    private static TestSpec testTokenNullLast() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .document(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "subfield",
                          FieldDefinitionBuilder.builder()
                              .token(TokenFieldDefinitionBuilder.builder().build())
                              .build())
                      .build())
              .build(),
          new UserFieldSortOptions(SortOrder.ASC, SortSelector.MIN, NullEmptySortPosition.HIGHEST),
          ValidateLuceneSortTest::extractTokenValues,
          (o1, o2) -> {
            if (o1 == null && o2 == null) {
              return 0;
            }
            if (o1 == null) {
              return 1;
            } // null last
            if (o2 == null) {
              return -1;
            }
            BsonString s1 = (BsonString) o1;
            BsonString s2 = (BsonString) o2;
            return s1.compareTo(s2);
          },
          List.of(
              new BsonDocument("subfield", new BsonString("zebra")), // has subfield
              new BsonDocument(), // missing subfield (null)
              new BsonDocument("subfield", new BsonString("apple")), // has subfield
              new BsonDocument(), // missing subfield (null)
              new BsonDocument(), // missing subfield (null)
              new BsonDocument("subfield", new BsonString("banana")) // has subfield
              ),
          Optional.of("f.subfield"),
          Optional.of(3L));
    }

    // Test case 11: long null last
    private static TestSpec testLongNullLast() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .document(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "subfield",
                          FieldDefinitionBuilder.builder()
                              .number(
                                  NumericFieldDefinitionBuilder.builder()
                                      .indexIntegers(true)
                                      .representation(NumericFieldOptions.Representation.INT64)
                                      .buildNumberField())
                              .build())
                      .build())
              .build(),
          new UserFieldSortOptions(SortOrder.ASC, SortSelector.MIN, NullEmptySortPosition.HIGHEST),
          ValidateLuceneSortTest::extractNumberValues,
          (o1, o2) -> {
            if (o1 == null && o2 == null) {
              return 0;
            }
            if (o1 == null) {
              return 1;
            } // null last
            if (o2 == null) {
              return -1;
            }
            return Long.compare((Long) o1, (Long) o2);
          },
          List.of(
              new BsonDocument("subfield", new BsonInt64(2)), // has subfield
              new BsonDocument(), // missing subfield (null)
              new BsonDocument("subfield", new BsonInt64(5)), // has subfield
              new BsonDocument(), // missing subfield (null)
              new BsonDocument(), // missing subfield (null)
              new BsonDocument("subfield", new BsonInt64(1)) // has subfield
              ),
          Optional.of("f.subfield"),
          Optional.of(3L));
    }

    // Test case 12: date null last
    private static TestSpec testDateNullLast() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .document(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "subfield",
                          FieldDefinitionBuilder.builder()
                              .date(DateFieldDefinitionBuilder.builder().build())
                              .build())
                      .build())
              .build(),
          new UserFieldSortOptions(SortOrder.ASC, SortSelector.MIN, NullEmptySortPosition.HIGHEST),
          ValidateLuceneSortTest::extractDateValues,
          (o1, o2) -> {
            if (o1 == null && o2 == null) {
              return 0;
            }
            if (o1 == null) {
              return 1;
            } // null last
            if (o2 == null) {
              return -1;
            }
            return Long.compare((Long) o1, (Long) o2);
          },
          List.of(
              new BsonDocument("subfield", new BsonDateTime(2)), // has subfield
              new BsonDocument(), // missing subfield (null)
              new BsonDocument("subfield", new BsonDateTime(5)), // has subfield
              new BsonDocument(), // missing subfield (null)
              new BsonDocument(), // missing subfield (null)
              new BsonDocument("subfield", new BsonDateTime(1)) // has subfield
              ),
          Optional.of("f.subfield"),
          Optional.of(3L));
    }

    // Test case 13: MIN selector
    private static TestSpec testTokenMinSelector() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .token(TokenFieldDefinitionBuilder.builder().build())
              .build(),
          new UserFieldSortOptions(SortOrder.ASC, SortSelector.MIN, NullEmptySortPosition.LOWEST),
          ValidateLuceneSortTest::extractTokenValues,
          (o1, o2) -> {
            BsonString s1 = (BsonString) o1;
            BsonString s2 = (BsonString) o2;
            return s1.compareTo(s2);
          },
          List.of(
              // "single"
              new BsonString("single"),
              // array - min is "apple"
              new BsonArray(
                  List.of(
                      new BsonString("zebra"), new BsonString("apple"), new BsonString("orange"))),
              // array - min is "banana"
              new BsonArray(List.of(new BsonString("banana"), new BsonString("cherry"))),
              // "delta"
              new BsonString("delta"),
              // "echod"
              new BsonArray(List.of(new BsonString("echo")))));
    }

    private static TestSpec testLongMinSelector() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .number(
                  NumericFieldDefinitionBuilder.builder()
                      .indexIntegers(true)
                      .representation(NumericFieldOptions.Representation.INT64)
                      .buildNumberField())
              .build(),
          new UserFieldSortOptions(SortOrder.ASC, SortSelector.MIN, NullEmptySortPosition.LOWEST),
          ValidateLuceneSortTest::extractNumberValues,
          Comparator.comparingLong(o -> (Long) o),
          List.of(
              // 10
              new BsonInt64(10),
              // array - min is 5
              new BsonArray(List.of(new BsonInt64(5), new BsonInt64(6), new BsonInt64(7))),
              // array - min is 12
              new BsonArray(List.of(new BsonInt64(12), new BsonInt64(13))),
              // 7
              new BsonInt64(7),
              // 6
              new BsonArray(List.of(new BsonInt64(6)))));
    }

    // Test case 14: MAX selector
    private static TestSpec testTokenMaxSelector() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .token(TokenFieldDefinitionBuilder.builder().build())
              .build(),
          new UserFieldSortOptions(SortOrder.DESC, SortSelector.MAX, NullEmptySortPosition.LOWEST),
          ValidateLuceneSortTest::extractTokenValues,
          (o1, o2) -> {
            BsonString s1 = (BsonString) o1;
            BsonString s2 = (BsonString) o2;
            return s1.compareTo(s2);
          },
          List.of(
              // "single"
              new BsonString("single"),
              // array - max is "zebra"
              new BsonArray(
                  List.of(
                      new BsonString("zebra"), new BsonString("apple"), new BsonString("orange"))),
              // array - max is "cherry"
              new BsonArray(List.of(new BsonString("banana"), new BsonString("cherry"))),
              // "delta"
              new BsonString("delta"),
              // "echod"
              new BsonArray(List.of(new BsonString("echo")))));
    }

    private static TestSpec testLongMaxSelector() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .number(
                  NumericFieldDefinitionBuilder.builder()
                      .indexIntegers(true)
                      .representation(NumericFieldOptions.Representation.INT64)
                      .buildNumberField())
              .build(),
          new UserFieldSortOptions(SortOrder.DESC, SortSelector.MAX, NullEmptySortPosition.LOWEST),
          ValidateLuceneSortTest::extractNumberValues,
          Comparator.comparingLong(o -> (Long) o),
          List.of(
              // 10
              new BsonInt64(10),
              // array - max is 7
              new BsonArray(List.of(new BsonInt64(5), new BsonInt64(6), new BsonInt64(7))),
              // array - max is 13
              new BsonArray(List.of(new BsonInt64(12), new BsonInt64(13))),
              // 7
              new BsonInt64(7),
              // 6
              new BsonArray(List.of(new BsonInt64(6)))));
    }

    // Test case: embedded token
    private static TestSpec testEmbeddedToken() {
      return new TestSpec(
          FieldDefinitionBuilder.builder()
              .embeddedDocuments(
                  EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
              .document(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "subfield",
                          FieldDefinitionBuilder.builder()
                              .document(
                                  DocumentFieldDefinitionBuilder.builder()
                                      .field(
                                          "token",
                                          FieldDefinitionBuilder.builder()
                                              .token(TokenFieldDefinitionBuilder.builder().build())
                                              .build())
                                      .build())
                              .build())
                      .build())
              .build(),
          UserFieldSortOptions.DEFAULT_ASC,
          ValidateLuceneSortTest::extractEmbeddedTokenValues,
          (o1, o2) -> {
            BsonString s1 = (BsonString) o1;
            BsonString s2 = (BsonString) o2;
            return s1.compareTo(s2);
          },
          List.of(
              new BsonDocument("subfield", new BsonDocument("token", new BsonString("delta"))),
              new BsonDocument("subfield", new BsonDocument("token", new BsonString("alpha"))),
              new BsonDocument("subfield", new BsonDocument("token", new BsonString("charlie"))),
              new BsonDocument("subfield", new BsonDocument("token", new BsonString("bravo")))),
          Optional.of("f.subfield.token"),
          Optional.empty());
    }
  }

  public static class ValidateMultiTypeSortTest {

    @Test
    public void createIndexSort_multiTypeField_producesMqlMixedSort() {
      SearchFieldDefinitionResolver mockResolver = mock(SearchFieldDefinitionResolver.class);
      when(mockResolver.getIndexCapabilities()).thenReturn(SearchIndexCapabilities.CURRENT);
      when(mockResolver.getFieldDefinition(any(), any()))
          .thenReturn(
              Optional.of(
                  FieldDefinitionBuilder.builder()
                      .token(TokenFieldDefinitionBuilder.builder().build())
                      .number(
                          NumericFieldDefinitionBuilder.builder()
                              .indexIntegers(true)
                              .representation(NumericFieldOptions.Representation.INT64)
                              .buildNumberField())
                      .build()));

      LuceneIndexSortFactory sortFactory = new LuceneIndexSortFactory(mockResolver);
      Sort sortSpec =
          new Sort(
              ImmutableList.of(
                  new MongotSortField(
                      FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC)));
      org.apache.lucene.search.Sort actualSort = sortFactory.createIndexSort(sortSpec);

      SortField[] sortFields = actualSort.getSort();
      // MqlMixedSort handles null/missing internally; no nullness prefix for multi-type fields.
      assertThat(sortFields.length).isEqualTo(1);
      assertThat(sortFields[0]).isInstanceOf(MqlMixedSort.class);
    }

    @Test
    public void createIndexSort_multiTypeFieldWithoutNumericOrDate_noNullnessPrefix() {
      SearchFieldDefinitionResolver mockResolver = mock(SearchFieldDefinitionResolver.class);
      when(mockResolver.getIndexCapabilities()).thenReturn(SearchIndexCapabilities.CURRENT);
      when(mockResolver.getFieldDefinition(any(), any()))
          .thenReturn(
              Optional.of(
                  FieldDefinitionBuilder.builder()
                      .token(TokenFieldDefinitionBuilder.builder().build())
                      .bool(BooleanFieldDefinitionBuilder.builder().build())
                      .build()));

      LuceneIndexSortFactory sortFactory = new LuceneIndexSortFactory(mockResolver);
      Sort sortSpec =
          new Sort(
              ImmutableList.of(
                  new MongotSortField(
                      FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC)));
      org.apache.lucene.search.Sort actualSort = sortFactory.createIndexSort(sortSpec);

      SortField[] sortFields = actualSort.getSort();
      // TOKEN + BOOLEAN: no numeric/date types, so no nullness prefix
      assertThat(sortFields.length).isEqualTo(1);
      assertThat(sortFields[0]).isInstanceOf(MqlMixedSort.class);
    }
  }

  public static class ValidateMetricsTests {
    private Directory directory;
    private IndexWriter indexWriter;
    private LuceneSearcherManager searcherManager;

    @Before
    public void setUp() throws Exception {
      this.directory = new ByteBuffersDirectory();
      this.indexWriter = new IndexWriter(this.directory, new IndexWriterConfig());
    }

    @After
    public void tearDown() throws Exception {
      if (this.searcherManager != null) {
        this.searcherManager.close();
      }
      if (this.indexWriter != null) {
        this.indexWriter.close();
      }
      if (this.directory != null) {
        this.directory.close();
      }
    }

    @Test
    public void testBenefitFromIndexSortCounter_CanBenefit() throws Exception {
      var queryingMetricsUpdater = SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH);

      var reader =
          LuceneSearchIndexReader.create(
              mock(LuceneSearchQueryFactoryDistributor.class),
              mock(LuceneSearcherManager.class),
              SearchIndex.mockDefinitionBuilder().build(),
              mock(LuceneHighlighterContext.class),
              mock(LuceneFacetContext.class),
              queryingMetricsUpdater,
              mock(LuceneSearchManagerFactory.class),
              Optional.empty(),
              0,
              FeatureFlags.getDefault(),
              new DynamicFeatureFlagRegistry(
                  Optional.empty(), Optional.empty(),
                  Optional.empty(), Optional.empty()));

      double initialCount = queryingMetricsUpdater.getBenefitFromIndexSortCounter().count();

      // Create matching sorts
      var luceneSort = new org.apache.lucene.search.Sort(new SortField("foo", SortField.Type.LONG));
      var indexSort = new org.apache.lucene.search.Sort(new SortField("foo", SortField.Type.LONG));

      // Test the method directly
      reader.trackIndexSortMetrics(Optional.of(luceneSort), Optional.of(indexSort));

      assertThat(queryingMetricsUpdater.getBenefitFromIndexSortCounter().count())
          .isEqualTo(initialCount + 1);

      // Test not matching sorts
      var luceneSort2 =
          new org.apache.lucene.search.Sort(new SortField("foo1", SortField.Type.LONG));
      reader.trackIndexSortMetrics(Optional.of(luceneSort2), Optional.of(indexSort));

      // the counter shouldn't change
      assertThat(queryingMetricsUpdater.getBenefitFromIndexSortCounter().count())
          .isEqualTo(initialCount + 1);
    }
  }
}
