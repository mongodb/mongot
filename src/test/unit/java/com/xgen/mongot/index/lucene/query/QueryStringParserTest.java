package com.xgen.mongot.index.lucene.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.analyzer.wrapper.QueryAnalyzerWrapper;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.synonym.SynonymRegistryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Optional;
import java.util.UUID;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

public class QueryStringParserTest {

  private static final String DEFAULT_FIELD = "defaultField";

  private static final SearchIndexDefinition embeddedIndexDefinition =
      SearchIndexDefinitionBuilder.builder()
          .indexId(new ObjectId())
          .name("default")
          .database("mock_database")
          .lastObservedCollectionName("mock_collection")
          .collectionUuid(UUID.randomUUID())
          .dynamicMapping()
          .mappings(
              DocumentFieldDefinitionBuilder.builder()
                  .field(
                      "case_sensitive",
                      FieldDefinitionBuilder.builder()
                          .string(
                              StringFieldDefinitionBuilder.builder()
                                  .analyzerName("lucene.keyword")
                                  .build())
                          .build())
                  .field(
                      "teachers",
                      FieldDefinitionBuilder.builder()
                          .embeddedDocuments(
                              EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                  .field(
                                      "firstName",
                                      FieldDefinitionBuilder.builder()
                                          .string(StringFieldDefinitionBuilder.builder().build())
                                          .build())
                                  .build())
                          .build())
                  .build())
          .searchAnalyzerName("lucene.standard")
          .build();

  private static final SearchIndexDefinition indexDefinition =
      SearchIndexDefinitionBuilder.builder()
          .indexId(new ObjectId())
          .name("default")
          .database("mock_database")
          .lastObservedCollectionName("mock_collection")
          .collectionUuid(UUID.randomUUID())
          .dynamicMapping()
          .mappings(
              DocumentFieldDefinitionBuilder.builder()
                  .field(
                      "case_sensitive",
                      FieldDefinitionBuilder.builder()
                          .string(
                              StringFieldDefinitionBuilder.builder()
                                  .analyzerName("lucene.keyword")
                                  .build())
                          .build())
                  .build())
          .searchAnalyzerName("lucene.standard")
          .build();

  private static Query createQuery(String query, String field, Optional<String> embeddedRoot)
      throws InvalidQueryException {
    return createQuery(query, field, embeddedRoot, indexDefinition);
  }

  private static Query createQuery(
      String query,
      String field,
      Optional<String> embeddedRoot,
      SearchIndexDefinition indexDefinition)
      throws InvalidQueryException {

    QueryAnalyzerWrapper wrapper =
        LuceneAnalyzer.queryAnalyzer(indexDefinition, AnalyzerRegistryBuilder.empty());

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            AnalyzerRegistryBuilder.empty(),
            wrapper,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            SynonymRegistryBuilder.empty(),
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());

    SingleQueryContext singleQueryContext =
        SingleQueryContext.createQueryRoot(mock(IndexReader.class));

    AllDocsQueryFactory allDocsQueryFactory = new AllDocsQueryFactory(queryFactoryContext);

    return QueryStringParser.createQuery(
        allDocsQueryFactory,
        wrapper,
        singleQueryContext,
        query,
        field,
        embeddedRoot.map(FieldPath::parse));
  }

  @Test
  public void testCreateQuery() throws InvalidQueryException {
    Query result = createQuery("Foobar", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/defaultField:foobar", result.toString());
  }

  @Test
  public void testCreateQueryCaseSensitive() throws InvalidQueryException {
    Query result = createQuery("Foobar", "case_sensitive", Optional.empty());

    assertEquals("$type:string/case_sensitive:Foobar", result.toString());
  }

  @Test
  public void testCreateQueryEmbeddedRoot() throws InvalidQueryException {
    Query result = createQuery("foo AND other:bar", DEFAULT_FIELD, Optional.of("root"));

    assertEquals(
        "+$embedded:4/root/$type:string/defaultField:foo +$embedded:4/root/$type:string/other:bar",
        result.toString());
  }

  @Test
  public void testCreateQueryPrefix() throws InvalidQueryException {
    Query result = createQuery("field:PREFIX*", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/field:prefix*", result.toString());
  }

  @Test
  public void testCreateQueryPrefixCaseSensitive() throws InvalidQueryException {
    Query result = createQuery("case_sensitive:PREFIX*", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/case_sensitive:PREFIX*", result.toString());
  }

  @Test
  public void testCreateQueryWildcard() throws InvalidQueryException {
    Query result = createQuery("field:WILD*card", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/field:wild*card", result.toString());
  }

  @Test
  public void testCreateQueryWildcardCaseSensitive() throws InvalidQueryException {
    Query result = createQuery("case_sensitive:WILD*card", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/case_sensitive:WILD*card", result.toString());
  }

  @Test
  public void testMatchAllDocsWithEmbeddedIndex() throws InvalidQueryException {
    Query result = createQuery("*:*", DEFAULT_FIELD, Optional.empty(), embeddedIndexDefinition);

    assertEquals("$meta/embeddedRoot:T", result.toString());
  }

  @Test
  public void testMatchAllDocsNoEmbeddedIndex() throws InvalidQueryException {
    Query result = createQuery("*:*", DEFAULT_FIELD, Optional.empty());

    assertEquals("*:*", result.toString());
  }

  @Test
  public void testAsteriskFieldTranslatedToLuceneField() throws InvalidQueryException {
    Query result = createQuery("*:foo", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/*:foo", result.toString());
  }

  @Test
  public void testCreateQueryFuzzy() throws InvalidQueryException {
    Query result = createQuery("approx:TEST~", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/approx:test~2", result.toString());
  }

  @Test
  public void testCreateQueryFuzzyCaseSensitive() throws InvalidQueryException {
    Query result = createQuery("case_sensitive:TEST~", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/case_sensitive:TEST~2", result.toString());
  }

  @Test
  public void testCreateQueryRange() throws InvalidQueryException {
    Query result = createQuery("range:[APPLE TO ZEBRA]", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/range:[apple TO zebra]", result.toString());
  }

  @Test
  public void testCreateQueryRangeCaseSensitive() throws InvalidQueryException {
    Query result = createQuery("case_sensitive:[APPLE TO ZEBRA]", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/case_sensitive:[APPLE TO ZEBRA]", result.toString());
  }

  @Test
  public void testCreateQueryRangeOpen() throws InvalidQueryException {
    Query result = createQuery("range:[* TO *]", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/range:[* TO *]", result.toString());
  }

  @Test
  public void testCreateQueryRangeExclusive() throws InvalidQueryException {
    Query result = createQuery("range:{APPLE TO *]", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/range:{apple TO *]", result.toString());
  }

  @Test
  public void testCreateRegex() throws InvalidQueryException {
    Query result = createQuery("regex:/REG?LAR*exp/", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/regex:/reg?lar*exp/", result.toString());
  }

  @Test
  public void testCreateRegexCaseSensitive() throws InvalidQueryException {
    Query result = createQuery("case_sensitive:/REG?LAR*exp/", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/case_sensitive:/REG?LAR*exp/", result.toString());
  }

  @Test
  public void createRegex_invalidTerm_throwsParserException() {
    Exception exception =
        assertThrows(
            InvalidQueryException.class,
            () -> createQuery("case_sensitive:/REG?LAR*exp(def/", DEFAULT_FIELD, Optional.empty()));

    assertTrue(exception.getMessage().contains("is not a valid regular expression"));
  }

  @Test
  public void testDateStringsNotNormalized() throws InvalidQueryException {
    Query result = createQuery("[6/1/2005 TO 6/4/2005]", DEFAULT_FIELD, Optional.empty());

    assertEquals("$type:string/defaultField:[6/1/2005 TO 6/4/2005]", result.toString());
  }
}
