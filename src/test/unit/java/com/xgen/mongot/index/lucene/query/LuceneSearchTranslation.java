package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.synonym.LuceneSynonymRegistry;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

/**
 * Tests helper class which sets up a real Lucene index and translates search operators to Lucene
 * queries.
 */
class LuceneSearchTranslation {
  private final AnalyzerRegistry analyzerRegistry;
  private final SearchIndexDefinition indexDefinition;
  private final SynonymRegistry synonymRegistry;
  private final FeatureFlags featureFlags;
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;
  private Directory directory;
  private IndexWriter writer;
  private int counter;

  /**
   * TestDocument
   *
   * @param textFields Will be tokenized.
   * @param stringFields Won't be tokenized.
   */
  record TestDocument(Map<String, String> textFields, Map<String, String> stringFields) {}

  private void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    this.directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    this.writer = new IndexWriter(this.directory, new IndexWriterConfig());
    this.writer.commit();
    this.counter = 0;
  }

  private void tearDown() throws IOException {
    this.writer.close();
    this.directory.close();
  }

  private LuceneSearchTranslation(
      Optional<String> analyzerName, Optional<DocumentFieldDefinition> mappings) {
    this(analyzerName, mappings, Optional.empty(), Optional.empty());
  }

  private LuceneSearchTranslation(
      Optional<String> analyzerName,
      Optional<DocumentFieldDefinition> mappings,
      Optional<Map<String, SynonymMappingDefinition>> synonyms,
      Optional<FeatureFlags> featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    this.analyzerRegistry = AnalyzerRegistryBuilder.empty();
    this.indexDefinition = getIndexDefinition(mappings, analyzerName);
    this.synonymRegistry =
        LuceneSynonymRegistry.create(
            this.analyzerRegistry, synonyms.orElseGet(Collections::emptyMap), Optional.empty());
    this.featureFlags = featureFlags.orElse(FeatureFlags.getDefault());
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
  }

  private LuceneSearchTranslation(
      Optional<String> analyzerName,
      Optional<DocumentFieldDefinition> mappings,
      Optional<Map<String, SynonymMappingDefinition>> synonyms,
      Optional<FeatureFlags> featureFlags) {
    this(analyzerName, mappings, synonyms, featureFlags, DynamicFeatureFlagRegistry.empty());
  }

  private LuceneSearchTranslation(
      AnalyzerRegistry analyzerRegistry,
      SearchIndexDefinition indexDefinition,
      SynonymRegistry synonymRegistry,
      FeatureFlags featureFlags) {
    this.analyzerRegistry = analyzerRegistry;
    this.indexDefinition = indexDefinition;
    this.synonymRegistry = synonymRegistry;
    this.featureFlags = featureFlags;
    this.dynamicFeatureFlagRegistry = DynamicFeatureFlagRegistry.empty();
  }

  private LuceneSearchTranslation(
      Optional<FeatureFlags> featureFlags, DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    this.analyzerRegistry = AnalyzerRegistryBuilder.empty();
    this.indexDefinition = SearchIndexDefinitionBuilder.VALID_INDEX;
    this.synonymRegistry =
        LuceneSynonymRegistry.create(
            this.analyzerRegistry, Collections.emptyMap(), Optional.empty());
    this.featureFlags = featureFlags.orElse(FeatureFlags.getDefault());
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
  }

  static LuceneSearchTranslation get() {
    return new LuceneSearchTranslation(Optional.empty(), Optional.empty());
  }

  static LuceneSearchTranslation analyzer(String analyzerName) {
    return new LuceneSearchTranslation(Optional.of(analyzerName), Optional.empty());
  }

  static LuceneSearchTranslation mapped(DocumentFieldDefinition mapping) {
    return new LuceneSearchTranslation(Optional.empty(), Optional.of(mapping));
  }

  static LuceneSearchTranslation gated(FeatureFlags featureFlags) {
    return new LuceneSearchTranslation(
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(featureFlags));
  }

  static LuceneSearchTranslation gatedWithDynamicFeatureFlags(DynamicFeatureFlags dff) {
    DynamicFeatureFlagConfig config =
        new DynamicFeatureFlagConfig(
            dff.getName(),
            DynamicFeatureFlagConfig.Phase.ENABLED,
            List.of(),
            List.of(),
            0,
            DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);
    DynamicFeatureFlagRegistry registry =
        new DynamicFeatureFlagRegistry(
            Optional.of(List.of(config)),
            Optional.empty(),
            Optional.empty(),
            Optional.of(new ObjectId()));
    return new LuceneSearchTranslation(Optional.empty(), registry);
  }

  static LuceneSearchTranslation featureFlagWithMapping(
      FeatureFlags featureFlags, DocumentFieldDefinition mapping) {
    return new LuceneSearchTranslation(
        Optional.empty(),
        Optional.of(mapping),
        Optional.empty(),
        Optional.of(featureFlags),
        DynamicFeatureFlagRegistry.empty());
  }

  /**
   * Same as {@link #mapped(DocumentFieldDefinition)} but with {@link
   * DynamicFeatureFlags#DISABLE_BULK_SCORER_QUERY_FOR_EMBEDDED_DOCUMENT_CHILD} explicitly disabled
   * (cluster phase), so embedded child queries are not wrapped with {@code DisableBulkScorerQuery}.
   */
  static LuceneSearchTranslation mappedWithDisableBulkScorerQueryForEmbeddedDocumentChildDisabled(
      DocumentFieldDefinition mapping) {
    DynamicFeatureFlagConfig config =
        new DynamicFeatureFlagConfig(
            DynamicFeatureFlags.DISABLE_BULK_SCORER_QUERY_FOR_EMBEDDED_DOCUMENT_CHILD.getName(),
            DynamicFeatureFlagConfig.Phase.DISABLED,
            List.of(),
            List.of(),
            0,
            DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);
    DynamicFeatureFlagRegistry registry =
        new DynamicFeatureFlagRegistry(
            Optional.of(List.of(config)),
            Optional.empty(),
            Optional.empty(),
            Optional.of(new ObjectId()));
    return new LuceneSearchTranslation(
        Optional.empty(), Optional.of(mapping), Optional.empty(), Optional.empty(), registry);
  }

  static LuceneSearchTranslation synonyms(
      AnalyzerRegistry registry,
      SearchIndexDefinition indexDefinition,
      SynonymRegistry synonymRegistry) {
    return new LuceneSearchTranslation(
        registry, indexDefinition, synonymRegistry, FeatureFlags.getDefault());
  }

  void assertTranslatedTo(Operator operator, Query expected)
      throws InvalidQueryException, IOException {
    Assert.assertEquals("Lucene query:", expected, translate(operator));
  }

  void assertTranslatedToWithEmbeddedRoot(
      Operator operator, Optional<FieldPath> embeddedRoot, Query expected)
      throws InvalidQueryException, IOException {
    Assert.assertEquals(
        "Lucene query:", expected.toString(), translate(operator, embeddedRoot).toString());
  }

  void assertTranslationThrows(Operator operator) {
    Assert.assertThrows(InvalidQueryException.class, () -> translate(operator));
  }

  Query translate(Operator operator) throws InvalidQueryException, IOException {
    setUp();
    return getLuceneQuery(operator, Optional.empty());
  }

  Query translate(Operator operator, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException, IOException {
    setUp();
    return getLuceneQuery(operator, embeddedRoot);
  }

  /** Produces a query with some documents indexed. */
  Query translateWithIndexedDocuments(Operator operator, List<TestDocument> docs)
      throws IOException, InvalidQueryException {
    setUp();

    for (TestDocument doc : docs) {
      Document luceneDoc = new Document();
      for (Map.Entry<String, String> kv : doc.textFields.entrySet()) {
        luceneDoc.add(new TextField(kv.getKey(), kv.getValue(), Field.Store.NO));
      }
      for (Map.Entry<String, String> kv : doc.stringFields.entrySet()) {
        luceneDoc.add(new StringField(kv.getKey(), kv.getValue(), Field.Store.NO));
      }
      index(luceneDoc);
    }

    return getLuceneQuery(operator, Optional.empty());
  }

  Query translateWithIndexedFields(Operator operator, List<String> fieldNames)
      throws IOException, InvalidQueryException {
    setUp();
    List<Document> docs = fieldNames.stream().map(this::stringFieldDocument).toList();
    for (Document doc : docs) {
      index(doc);
    }

    return getLuceneQuery(operator, Optional.empty());
  }

  private Query getLuceneQuery(Operator operator, Optional<FieldPath> embeddedRoot)
      throws IOException, InvalidQueryException {
    LuceneSearchQueryFactoryDistributor factory =
        LuceneSearchQueryFactoryDistributor.create(
            this.indexDefinition,
            IndexFormatVersion.CURRENT,
            this.analyzerRegistry,
            this.synonymRegistry,
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            false,
            this.featureFlags,
            this.dynamicFeatureFlagRegistry);
    try (var reader = DirectoryReader.open(this.directory)) {
      return factory.createQuery(
          operator,
          reader,
          embeddedRoot.map(ReturnScope::new),
          QueryOptimizationFlags.DEFAULT_OPTIONS);
    } finally {
      tearDown();
    }
  }

  private static SearchIndexDefinition getIndexDefinition(
      Optional<DocumentFieldDefinition> optionalMappings, Optional<String> analyzerName) {
    DocumentFieldDefinition rootMappings =
        optionalMappings.orElse(DocumentFieldDefinitionBuilder.builder().dynamic(true).build());

    var builder = SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(rootMappings);
    analyzerName.ifPresent(
        analyzer -> {
          builder.analyzerName(analyzer);
          builder.searchAnalyzerName(analyzer);
        });

    return builder.build();
  }

  private void index(Document doc) throws IOException {
    int id = this.counter++;
    doc.add(new StoredField(FieldName.MetaField.ID.getLuceneFieldName(), id));
    this.writer.addDocument(doc);
    this.writer.commit();
  }

  private Document stringFieldDocument(String field) {
    Document doc = new Document();
    doc.add(
        new TextField(
            FieldName.TypeField.STRING.getLuceneFieldName(FieldPath.parse(field), Optional.empty()),
            "_",
            Field.Store.NO));
    return doc;
  }
}
