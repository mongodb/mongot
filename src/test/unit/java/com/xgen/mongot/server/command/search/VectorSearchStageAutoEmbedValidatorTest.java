package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import java.util.Map;
import org.bson.BsonDocument;
import org.junit.Test;

public class VectorSearchStageAutoEmbedValidatorTest {

  private static final FieldPath DESCRIPTION = FieldPath.parse("description");
  private static final Map<FieldPath, FieldPath> EMBEDDED_DESCRIPTION =
      Map.of(DESCRIPTION, FieldPath.parse("_autoEmbed.description"));

  @Test
  public void validateNotReservedAutoEmbedPath_sourcePath_passes() throws Exception {
    VectorSearchStageAutoEmbedValidator.validateNotReservedAutoEmbedPath(DESCRIPTION);
  }

  @Test
  public void validateNotReservedAutoEmbedPath_reservedRoot_throws() {
    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateNotReservedAutoEmbedPath(
                    FieldPath.newRoot("_autoEmbed")));

    assertThat(exception)
        .hasMessageThat()
        .contains("Path '_autoEmbed' is reserved for internal automated embeddings fields");
  }

  @Test
  public void validateNotReservedAutoEmbedPath_reservedChild_throws() {
    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateNotReservedAutoEmbedPath(
                    FieldPath.parse("_autoEmbed.description")));

    assertThat(exception)
        .hasMessageThat()
        .contains(
            "Path '_autoEmbed.description' is reserved for internal automated embeddings fields");
  }

  @Test
  public void validateNotReservedAutoEmbedPath_similarPrefix_passes() throws Exception {
    VectorSearchStageAutoEmbedValidator.validateNotReservedAutoEmbedPath(
        FieldPath.parse("_autoEmbedded.description"));
  }

  @Test
  public void validateVectorSearchStage_autoEmbedSourcePath_throws() {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
                    vectorSearchStage("description", "{cost:{$lt:5}}"),
                    EMBEDDED_DESCRIPTION,
                    served));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "$vectorSearch cannot query type:autoEmbed fields in search index 'default'. Use the"
                + " vectorSearch operator in $search instead.");
  }

  @Test
  public void validateVectorSearchStage_autoEmbedVectorPath_throws() {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
                    vectorSearchStage("_autoEmbed.description", "{cost:{$lt:5}}"),
                    EMBEDDED_DESCRIPTION,
                    served));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Path '_autoEmbed.description' is reserved for internal automated embeddings fields."
                + " Query the field by its source path instead.");
  }

  @Test
  public void validateVectorSearchStage_filterOnAutoEmbedVectorPath_throws() {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
                    vectorSearchStage("embedding", "{'_autoEmbed.description':{$exists:true}}"),
                    EMBEDDED_DESCRIPTION,
                    served));

    assertThat(exception)
        .hasMessageThat()
        .contains(
            "Path '_autoEmbed.description' is reserved for internal automated embeddings fields");
  }

  @Test
  public void validateVectorSearchStage_filterOnReservedRoot_throws() {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
                    vectorSearchStage("embedding", "{_autoEmbed:{$exists:true}}"),
                    EMBEDDED_DESCRIPTION,
                    served));

    assertThat(exception)
        .hasMessageThat()
        .contains("Path '_autoEmbed' is reserved for internal automated embeddings fields");
  }

  @Test
  public void validateVectorSearchStage_filterOnReservedChildPath_throws() {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
                    vectorSearchStage("embedding", "{'_autoEmbed.unknown':{$exists:true}}"),
                    EMBEDDED_DESCRIPTION,
                    served));

    assertThat(exception)
        .hasMessageThat()
        .contains("Path '_autoEmbed.unknown' is reserved for internal automated embeddings fields");
  }

  @Test
  public void validateVectorSearchStage_parentFilterOnReservedPath_throws() {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
                    vectorSearchStageWithParentFilter("embedding", "{_autoEmbed:{$exists:true}}"),
                    EMBEDDED_DESCRIPTION,
                    served));

    assertThat(exception)
        .hasMessageThat()
        .contains("Path '_autoEmbed' is reserved for internal automated embeddings fields");
  }

  @Test
  public void validateVectorSearchStage_nestedFilterOnAutoEmbedOnlyPath_throws() {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
                    vectorSearchStage(
                        "embedding", "{$or:[{cost:{$lt:5}},{description:{$exists:false}}]}"),
                    EMBEDDED_DESCRIPTION,
                    served));

    assertThat(exception).hasMessageThat().contains("Path 'description' is a type:autoEmbed field");
  }

  @Test
  public void validateVectorSearchStage_otherVectorPathAndFilter_passes() throws Exception {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
        vectorSearchStage("embedding", "{cost:{$lt:5}}"), EMBEDDED_DESCRIPTION, served);
  }

  @Test
  public void validateVectorSearchStage_catalogOnlySourcePath_passes() throws Exception {
    // The catalog still lists description as autoEmbed, but the served generation does not embed
    // it.
    SearchIndexDefinition served =
        definition(DocumentFieldDefinitionBuilder.builder().dynamic(false).build());

    VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
        vectorSearchStage("description", "{cost:{$lt:5}}"), EMBEDDED_DESCRIPTION, served);
  }

  @Test
  public void validateVectorSearchStage_catalogOnlyUnmappedSourcePath_passes() throws Exception {
    // Without a materialized-view schema the vector path falls back to the source path.
    SearchIndexDefinition served =
        definition(DocumentFieldDefinitionBuilder.builder().dynamic(false).build());

    VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
        vectorSearchStage("description", "{cost:{$lt:5}}"),
        Map.of(DESCRIPTION, DESCRIPTION),
        served);
  }

  @Test
  public void validateVectorSearchStage_filterOnUnmappedSourcePathServedAsVector_throws() {
    // Without a materialized-view schema the generated vector is served at the source path.
    SearchIndexDefinition served =
        definition(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "description",
                    FieldDefinitionBuilder.builder()
                        .searchIndexVectorField(
                            new SearchIndexVectorFieldDefinition(
                                new VectorFieldSpecification(
                                    2,
                                    VectorSimilarity.DOT_PRODUCT,
                                    VectorQuantization.NONE,
                                    new VectorIndexingAlgorithm.HnswIndexingAlgorithm())))
                        .build())
                .build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
                    vectorSearchStage("embedding", "{description:{$exists:false}}"),
                    Map.of(DESCRIPTION, DESCRIPTION),
                    served));

    assertThat(exception).hasMessageThat().contains("is a type:autoEmbed field");
  }

  @Test
  public void validateVectorSearchStage_filterOnPathServedWithAnotherType_passes()
      throws Exception {
    SearchIndexDefinition served =
        definition(
            derivedDescriptionMappings()
                .field(
                    "description",
                    FieldDefinitionBuilder.builder()
                        .string(StringFieldDefinitionBuilder.builder().build())
                        .build())
                .build());

    VectorSearchStageAutoEmbedValidator.validateVectorSearchStage(
        vectorSearchStage("embedding", "{description:{$exists:true}}"),
        EMBEDDED_DESCRIPTION,
        served);
  }

  private static VectorSearchQuery vectorSearchStageWithParentFilter(
      String path, String parentFilterJson) {
    try {
      return VectorSearchQuery.fromBson(
          BsonDocument.parse(
              String.format(
                  "{index:'default',path:'%s',queryVector:[1.0,2.0],numCandidates:10,limit:3,"
                      + "parentFilter:%s}",
                  path, parentFilterJson)));
    } catch (BsonParseException e) {
      throw new AssertionError(e);
    }
  }

  private static VectorSearchQuery vectorSearchStage(String path, String filterJson) {
    try {
      return VectorSearchQuery.fromBson(
          BsonDocument.parse(
              String.format(
                  "{index:'default',path:'%s',queryVector:[1.0,2.0],numCandidates:10,limit:3,"
                      + "filter:%s}",
                  path, filterJson)));
    } catch (BsonParseException e) {
      throw new AssertionError(e);
    }
  }

  private static DocumentFieldDefinitionBuilder derivedDescriptionMappings() {
    return DocumentFieldDefinitionBuilder.builder()
        .dynamic(false)
        .field(
            "_autoEmbed",
            FieldDefinitionBuilder.builder()
                .document(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(false)
                        .field(
                            "description",
                            FieldDefinitionBuilder.builder()
                                .searchIndexVectorField(
                                    new SearchIndexVectorFieldDefinition(
                                        new VectorFieldSpecification(
                                            2,
                                            VectorSimilarity.DOT_PRODUCT,
                                            VectorQuantization.NONE,
                                            new VectorIndexingAlgorithm.HnswIndexingAlgorithm())))
                                .build())
                        .build())
                .build());
  }

  private static SearchIndexDefinition definition(DocumentFieldDefinition mappings) {
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .mappings(mappings)
        .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
        .build();
  }
}
