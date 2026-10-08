package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.mongodb.MaterializedViewWriter.MV_DATABASE_NAME;

import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.IncompatibleIndexes;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.SearchQueryTimeMappingChecks;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.metrics.TestMeterRegistry;
import com.xgen.testing.mongot.server.command.search.definition.request.SearchCommandDefinitionBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Regression tests for {@code $search.vectorSearch} with a user-supplied {@code queryVector}
 * against an auto-embed field (CLOUDP-454556). The declared query path must be translated to the
 * materialized-view vector path and the vector dtype reconciled against the field's indexed
 * encoding, exactly like the text-embedding branch already does.
 */
public class SearchCommandAutoEmbedQueryVectorTest {

  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testCollection";
  private static final UUID COLLECTION_UUID = UUID.randomUUID();
  private static final String INDEX_NAME = "default";
  private static final int DIMENSIONS = 4;
  private static final FieldPath QUERY_PATH = FieldPath.parse("desc_embedding");
  private static final FieldPath PLAIN_VECTOR_PATH = FieldPath.parse("plain_vector");
  private static final String QUERY_VECTOR_JSON = "[0.1,0.2,0.3,0.4]";

  private static final SearchCommandsRegister.BootstrapperMetadata BOOTSTRAPPER_METADATA =
      new SearchCommandsRegister.BootstrapperMetadata(
          "testVersion",
          "localhost",
          () -> MongoDbServerInfo.EMPTY,
          FeatureFlags.getDefault(),
          DynamicFeatureFlagRegistry.empty());

  @Test
  public void queryVectorOnAutoEmbedField_isRetargetedToMaterializedViewPath() throws Exception {
    FieldPath matViewPath = FieldPath.parse("_autoEmbed.desc_embedding");
    Fixture fixture =
        fixture(QUERY_PATH, VectorAutoEmbedQuantization.FLOAT, Map.of(QUERY_PATH, matViewPath));

    VectorSearchCriteria criteria = translate(fixture, QUERY_PATH);

    // This is the same validation the Lucene query factory runs before it builds the vector query.
    // Before the fix it throws: desc_embedding must be indexed as "vector".
    validate(fixture.derivedDefinition, criteria);

    assertThat(criteria.path()).isEqualTo(matViewPath);
    assertThat(criteria.query()).isEmpty();
    assertThat(criteria.queryVector()).isPresent();
  }

  @Test
  public void queryVectorOnAutoEmbedField_mapsTheAutoEmbedSourcePath() throws Exception {
    // Production schema shape: the field is queried as "desc_embedding" but embeds source field
    // "description", so the materialized-view path must be resolved from the source path.
    FieldPath sourcePath = FieldPath.parse("description");
    FieldPath matViewPath = FieldPath.parse("_autoEmbed.description");
    Fixture fixture =
        fixture(sourcePath, VectorAutoEmbedQuantization.FLOAT, Map.of(sourcePath, matViewPath));

    VectorSearchCriteria criteria = translate(fixture, QUERY_PATH);

    validate(fixture.derivedDefinition, criteria);

    assertThat(criteria.path()).isEqualTo(matViewPath);
  }

  @Test
  public void floatQueryVectorOnScalarAutoEmbedField_isRejected() throws Exception {
    FieldPath matViewPath = FieldPath.parse("_autoEmbed.desc_embedding");
    Fixture fixture =
        fixture(QUERY_PATH, VectorAutoEmbedQuantization.SCALAR, Map.of(QUERY_PATH, matViewPath));

    InvalidQueryException error =
        Assert.assertThrows(InvalidQueryException.class, () -> translate(fixture, QUERY_PATH));

    assertThat(error).hasMessageThat().contains("autoEmbed quantization 'scalar'");
  }

  @Test
  public void queryVectorOnRegularVectorField_isLeftUnchanged() throws Exception {
    FieldPath matViewPath = FieldPath.parse("_autoEmbed.desc_embedding");
    Fixture fixture =
        fixture(QUERY_PATH, VectorAutoEmbedQuantization.FLOAT, Map.of(QUERY_PATH, matViewPath));

    VectorSearchCriteria criteria = translate(fixture, PLAIN_VECTOR_PATH);

    assertThat(criteria.path()).isEqualTo(PLAIN_VECTOR_PATH);
    // Regular type:vector fields are preserved in the derived definition and validate as-is.
    validate(fixture.derivedDefinition, criteria);
  }

  @Test
  public void emptyQueryOnAutoEmbedField_isRejected() throws Exception {
    FieldPath matViewPath = FieldPath.parse("_autoEmbed.desc_embedding");
    Fixture fixture =
        fixture(QUERY_PATH, VectorAutoEmbedQuantization.FLOAT, Map.of(QUERY_PATH, matViewPath));
    SearchQuery query =
        SearchQuery.fromBson(
            BsonDocument.parse(
                String.format(
                    "{\"vectorSearch\":{\"path\":\"%s\",\"query\":\"\","
                        + "\"numCandidates\":10,\"limit\":5}}",
                    QUERY_PATH)));

    InvalidQueryException error =
        Assert.assertThrows(
            InvalidQueryException.class,
            () -> fixture.command.maybeEmbedSearchQuery(query, fixture.derivedDefinition));

    assertThat(error).hasMessageThat().contains("'query' field cannot be empty");
  }

  private static VectorSearchCriteria translate(Fixture fixture, FieldPath path) throws Exception {
    SearchQuery query = SearchQuery.fromBson(vectorSearchQuery(path));
    SearchQuery translated =
        fixture.command.maybeEmbedSearchQuery(query, fixture.derivedDefinition);
    return ((VectorSearchOperator) ((OperatorQuery) translated).operator()).criteria();
  }

  private static void validate(
      SearchIndexDefinition derivedDefinition, VectorSearchCriteria criteria)
      throws InvalidQueryException {
    SearchQueryTimeMappingChecks checks =
        new SearchQueryTimeMappingChecks(
            derivedDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT));
    checks.validateVectorField(
        criteria.path(), Optional.empty(), criteria.queryVector().orElseThrow().numDimensions());
  }

  private static BsonDocument vectorSearchQuery(FieldPath path) {
    return BsonDocument.parse(
        String.format(
            "{\"vectorSearch\":{\"path\":\"%s\",\"queryVector\":%s,"
                + "\"numCandidates\":10,\"limit\":5}}",
            path, QUERY_VECTOR_JSON));
  }

  private static Fixture fixture(
      FieldPath sourcePath,
      VectorAutoEmbedQuantization quantization,
      Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping) {
    SearchIndexDefinition rawDefinition = rawAutoEmbedSearchDefinition(sourcePath, quantization);
    MaterializedViewSchemaMetadata schemaMetadata =
        new MaterializedViewSchemaMetadata(1L, autoEmbeddingFieldsMapping);
    SearchIndexDefinition derivedDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            rawDefinition, MV_DATABASE_NAME, UUID.randomUUID(), schemaMetadata);
    return new Fixture(commandWith(catalogWith(rawDefinition, schemaMetadata)), derivedDefinition);
  }

  private static SearchIndexDefinition rawAutoEmbedSearchDefinition(
      FieldPath sourcePath, VectorAutoEmbedQuantization quantization) {
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition(
            sourcePath,
            new VectorAutoEmbedFieldSpecification(
                DIMENSIONS,
                VectorSimilarity.DOT_PRODUCT,
                quantization,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                    new VectorFieldSpecification.HnswOptions(16, 100)),
                "voyage-3-large",
                "text"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                QUERY_PATH.toString(),
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                PLAIN_VECTOR_PATH.toString(),
                FieldDefinitionBuilder.builder()
                    .searchIndexVectorField(
                        new SearchIndexVectorFieldDefinition(
                            new VectorFieldSpecification(
                                DIMENSIONS,
                                VectorSimilarity.DOT_PRODUCT,
                                VectorQuantization.NONE,
                                new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                                    new VectorFieldSpecification.HnswOptions(16, 100)))))
                    .build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static SearchCommand commandWith(IndexCatalog indexCatalog) {
    return new SearchCommand(
        Mockito.mock(SearchCommand.Metrics.class),
        Mockito.mock(MongotCursorManager.class),
        indexCatalog,
        Mockito.mock(InitializedIndexCatalog.class),
        new IncompatibleIndexes(TestMeterRegistry.get()),
        SearchCommandDefinitionBuilder.builder()
            .db(DATABASE_NAME)
            .collectionName(COLLECTION_NAME)
            .collectionUuid(COLLECTION_UUID)
            .query(BsonDocument.parse("{\"text\":{\"path\":\"a\",\"query\":\"b\"}}"))
            .build(),
        BOOTSTRAPPER_METADATA,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        () -> null);
  }

  private static IndexCatalog catalogWith(
      SearchIndexDefinition rawDefinition, MaterializedViewSchemaMetadata schemaMetadata) {
    InitializedMaterializedViewIndex matViewIndex =
        Mockito.mock(InitializedMaterializedViewIndex.class);
    Mockito.when(matViewIndex.getSchemaMetadata()).thenReturn(schemaMetadata);

    MaterializedViewIndexGeneration matViewIndexGeneration =
        Mockito.mock(MaterializedViewIndexGeneration.class);
    Mockito.when(matViewIndexGeneration.getIndex()).thenReturn(matViewIndex);

    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        Mockito.mock(AutoEmbeddingIndexGeneration.class);
    Mockito.when(autoEmbeddingIndexGeneration.getType())
        .thenReturn(IndexDefinitionGeneration.Type.AUTO_EMBEDDING);
    Mockito.when(autoEmbeddingIndexGeneration.getDefinition()).thenReturn(rawDefinition);
    Mockito.when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(matViewIndexGeneration);

    IndexCatalog catalog = Mockito.mock(IndexCatalog.class);
    Mockito.when(catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
        .thenReturn(Optional.of(autoEmbeddingIndexGeneration));
    return catalog;
  }

  private record Fixture(SearchCommand command, SearchIndexDefinition derivedDefinition) {}
}
