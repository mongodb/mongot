package com.xgen.mongot.index.lucene.query.context;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerMeta;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.lucene.synonym.LuceneSynonymRegistry;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.synonym.SynonymRegistryBuilder;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.lucene.analysis.Analyzer;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class SearchQueryFactoryContextTest {
  @DataPoints
  public static IndexFormatVersion[] indexFormatVersions =
      new IndexFormatVersion[] {
        IndexFormatVersion.MIN_SUPPORTED_VERSION, IndexFormatVersion.CURRENT
      };

  @Theory
  public void testGetAllAnalyzersOnDynamicMapping(IndexFormatVersion indexFormatVersion) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("default")
            .database("mock_database")
            .lastObservedCollectionName("mock_collection")
            .collectionUuid(UUID.randomUUID())
            .dynamicMapping()
            .analyzerName("lucene.italian")
            .build();

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            AnalyzerRegistryBuilder.empty(),
            LuceneAnalyzer.queryAnalyzer(indexDefinition, AnalyzerRegistryBuilder.empty()),
            indexDefinition.createFieldDefinitionResolver(indexFormatVersion),
            SynonymRegistryBuilder.empty(),
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());
    var luceneFieldNames =
        queryFactoryContext.getAllLuceneFieldNames(
            new StringFieldPath(FieldPath.parse("a.b")), Optional.empty());

    assertThat(luceneFieldNames).containsExactly("$type:string/a.b");
  }

  @Theory
  public void testGetAnalyzerInvalidMulti(IndexFormatVersion indexFormatVersion) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("default")
            .database("mock_database")
            .lastObservedCollectionName("mock_collection")
            .collectionUuid(UUID.randomUUID())
            .dynamicMapping()
            .analyzerName("lucene.italian")
            .build();
    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            AnalyzerRegistryBuilder.empty(),
            LuceneAnalyzer.queryAnalyzer(indexDefinition, AnalyzerRegistryBuilder.empty()),
            indexDefinition.createFieldDefinitionResolver(indexFormatVersion),
            SynonymRegistryBuilder.empty(),
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());
    StringPath path = StringPathBuilder.withMulti("path", "invalid");

    assertThrows(
        InvalidQueryException.class, () -> queryFactoryContext.getAnalyzer(path, Optional.empty()));
    assertThrows(
        InvalidQueryException.class,
        () -> queryFactoryContext.getAnalyzerMeta(path, Optional.empty()));
  }

  @Theory
  public void testGetAnalyzerFallback(IndexFormatVersion indexFormatVersion)
      throws InvalidQueryException {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("default")
            .database("mock_database")
            .lastObservedCollectionName("mock_collection")
            .collectionUuid(UUID.randomUUID())
            .dynamicMapping()
            .analyzerName("lucene.italian")
            .build();
    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            AnalyzerRegistryBuilder.empty(),
            LuceneAnalyzer.queryAnalyzer(indexDefinition, AnalyzerRegistryBuilder.empty()),
            indexDefinition.createFieldDefinitionResolver(indexFormatVersion),
            SynonymRegistryBuilder.empty(),
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());
    StringPath path = StringPathBuilder.fieldPath("path");

    Analyzer result = queryFactoryContext.getAnalyzer(path, Optional.empty());
    AnalyzerMeta metaResult = queryFactoryContext.getAnalyzerMeta(path, Optional.empty());

    assertNotNull(result);
    assertEquals("lucene.italian", metaResult.getName());
  }

  @Theory
  public void testGetAllAnalyzersOnMultiField(IndexFormatVersion indexFormatVersion) {
    StringFieldDefinition fieldWithMulti =
        StringFieldDefinitionBuilder.builder()
            .multi(
                "kw", StringFieldDefinitionBuilder.builder().analyzerName("lucene.keyword").build())
            .build();

    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("default")
            .database("mock_database")
            .lastObservedCollectionName("mock_collection")
            .collectionUuid(UUID.randomUUID())
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field("a", FieldDefinitionBuilder.builder().string(fieldWithMulti).build())
                    .build())
            .build();

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            AnalyzerRegistryBuilder.empty(),
            LuceneAnalyzer.queryAnalyzer(indexDefinition, AnalyzerRegistryBuilder.empty()),
            indexDefinition.createFieldDefinitionResolver(indexFormatVersion),
            SynonymRegistryBuilder.empty(),
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());
    var luceneFields =
        queryFactoryContext.getAllLuceneFieldNames(
            new StringFieldPath(FieldPath.parse("a")), Optional.empty());

    assertThat(luceneFields).containsExactly("$type:string/a", "$multi/a.kw");
  }

  /**
   * Check that getSynonymAnalyzer returns the same analyzer otherwise used for search at a field.
   *
   * <p>Eventually, replace this test with one verifying that getSynonymAnalyzer does indeed return
   * a properly configured analyzer for the specified args.
   */
  @Theory
  public void testGetSynonymAnalyzerIsSearchAnalyzerToday(IndexFormatVersion indexFormatVersion)
      throws Exception {
    String fieldName = "someField";
    StringPath stringPath = new StringFieldPath(FieldPath.fromParts(fieldName));
    StringFieldDefinition stringField =
        StringFieldDefinitionBuilder.builder().analyzerName("lucene.english").build();
    SynonymMappingDefinition synonymDefinition =
        SynonymMappingDefinitionBuilder.builder()
            .name("en_synonyms")
            .analyzer("lucene.english")
            .synonymSourceDefinition("synonymsCollection")
            .build();

    SearchQueryFactoryContext queryFactoryContext =
        synonymQueryContext(synonymDefinition, fieldName, stringField, indexFormatVersion);

    AnalyzerMeta baseAnalyzer = queryFactoryContext.getAnalyzerMeta(stringPath, Optional.empty());

    Assert.assertEquals(
        "should return unmodified base analyzer",
        baseAnalyzer.getAnalyzer(),
        queryFactoryContext.getSynonymAnalyzer("en_synonyms", stringPath, Optional.empty()));
  }

  @Theory
  public void testGetSynonymAnalyzerThrowsWhenFieldUsesDifferentAnalyzer(
      IndexFormatVersion indexFormatVersion) {
    String fieldName = "someField";
    StringPath stringPath = new StringFieldPath(FieldPath.fromParts(fieldName));
    StringFieldDefinition stringField =
        StringFieldDefinitionBuilder.builder().analyzerName("lucene.french").build();
    SynonymMappingDefinition synonymDefinition =
        SynonymMappingDefinitionBuilder.builder()
            .name("en_synonyms")
            .analyzer("lucene.english")
            .synonymSourceDefinition("synonymsCollection")
            .build();

    SearchQueryFactoryContext queryFactoryContext =
        synonymQueryContext(synonymDefinition, fieldName, stringField, indexFormatVersion);

    try {
      queryFactoryContext.getSynonymAnalyzer("en_synonyms", stringPath, Optional.empty());
    } catch (InvalidQueryException e) {
      Assert.assertEquals(
          "should have correct message",
          "synonym mapping \"en_synonyms\" is only allowed with lucene.english analyzer, "
              + "field someField is analyzed with lucene.french analyzer",
          e.getMessage());
      return;
    }

    Assert.fail("this should be unreachable - this test should throw, catch, and exit");
  }

  @Theory
  public void testGetSynonymAnalyzerThrowsWhenFieldUsesDifferentSearchAnalyzer(
      IndexFormatVersion indexFormatVersion) {
    String fieldName = "someField";
    StringPath stringPath = new StringFieldPath(FieldPath.fromParts(fieldName));
    StringFieldDefinition stringField =
        StringFieldDefinitionBuilder.builder()
            .searchAnalyzerName("lucene.french")
            .analyzerName("lucene.english")
            .build();
    SynonymMappingDefinition synonymDefinition =
        SynonymMappingDefinitionBuilder.builder()
            .name("en_synonyms")
            .analyzer("lucene.english")
            .synonymSourceDefinition("synonymsCollection")
            .build();

    SearchQueryFactoryContext context =
        synonymQueryContext(synonymDefinition, fieldName, stringField, indexFormatVersion);
    InvalidQueryException ex =
        assertThrows(
            InvalidQueryException.class,
            () -> context.getSynonymAnalyzer("en_synonyms", stringPath, Optional.empty()));

    Assert.assertEquals(
        "should have correct message",
        "synonym mapping \"en_synonyms\" is only allowed with lucene.english analyzer, "
            + "field someField is analyzed with lucene.french analyzer",
        ex.getMessage());
  }

  @Theory
  public void testGetSynonymAnalyzerThrowsWhenSynonymMappingNameDoesNotExist(
      IndexFormatVersion indexFormatVersion) {
    String fieldName = "someField";
    StringPath stringPath = new StringFieldPath(FieldPath.fromParts(fieldName));

    SearchQueryFactoryContext queryFactoryContext =
        synonymQueryContext(
            List.of(),
            fieldName,
            StringFieldDefinitionBuilder.builder().analyzerName("lucene.english").build(),
            indexFormatVersion);

    try {
      queryFactoryContext.getSynonymAnalyzer("en_synonyms", stringPath, Optional.empty());
    } catch (InvalidQueryException e) {
      Assert.assertEquals(
          "should have correct message",
          "unknown synonym mapping name \"en_synonyms\"",
          e.getMessage());
      return;
    }

    Assert.fail("this should be unreachable - this test should throw, catch, and exit");
  }

  private static SearchQueryFactoryContext synonymQueryContext(
      SynonymMappingDefinition synonymMappingDefinition,
      String fieldName,
      StringFieldDefinition stringFieldDefinition,
      IndexFormatVersion indexFormatVersion) {
    return synonymQueryContext(
        List.of(synonymMappingDefinition), fieldName, stringFieldDefinition, indexFormatVersion);
  }

  private static SearchQueryFactoryContext synonymQueryContext(
      List<SynonymMappingDefinition> synonymMappingDefinitions,
      String fieldName,
      StringFieldDefinition stringFieldDefinition,
      IndexFormatVersion indexFormatVersion) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        fieldName,
                        FieldDefinitionBuilder.builder().string(stringFieldDefinition).build())
                    .build())
            .synonyms(synonymMappingDefinitions)
            .build();

    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(
            analyzerRegistry, indexDefinition.getSynonymMap(), Optional.empty());
    synonymMappingDefinitions.forEach(synonymRegistry::clear);

    return new SearchQueryFactoryContext(
        analyzerRegistry,
        LuceneAnalyzer.queryAnalyzer(indexDefinition, analyzerRegistry),
        indexDefinition.createFieldDefinitionResolver(indexFormatVersion),
        synonymRegistry,
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
        FeatureFlags.getDefault());
  }
}
