package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.VectorTextFieldDefinition;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.VectorSearchFilter.ClauseFilter;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import org.junit.Test;

public class VectorSearchFilterQueryFactoryTest {

  private static final IndexMetricsUpdater.QueryingMetricsUpdater metrics =
      new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

  @Test
  public void isAutoEmbedField_validationSucceeds() {
    FieldPath textPath = FieldPath.parse("content");
    FieldPath autoEmbedPath = FieldPath.parse("description");
    FieldPath filterPath = FieldPath.parse("category");

    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(
                List.of(
                    new VectorTextFieldDefinition("voyage-3-large", textPath),
                    new VectorAutoEmbedFieldDefinition("voyage-3-large", autoEmbedPath),
                    VectorIndexFilterFieldDefinition.create(filterPath)))
            .build();

    VectorQueryFactoryContext context =
        new VectorQueryFactoryContext(
            new VectorFieldDefinitionResolver(indexDefinition, IndexFormatVersion.CURRENT),
            FeatureFlags.getDefault(), metrics);

    assertTrue(context.isAutoEmbedField(textPath));
    assertTrue(context.isAutoEmbedField(autoEmbedPath));

    assertFalse(context.isAutoEmbedField(filterPath));
  }

  @Test
  public void filterOnNonAutoEmbedField_success()
      throws
      Exception {
    // Index with vector data field and separate filter field
    FieldPath vectorPath = FieldPath.parse("embedding");
    FieldPath filterPath = FieldPath.parse("category");

    var query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(vectorPath)
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .filter(
                        new ClauseFilter(
                            ClauseBuilder.simpleClause()
                                .path(filterPath) // Filter on NON-auto-embed field
                                .addOperator(
                                    MqlFilterOperatorBuilder.eq()
                                        .value(ValueBuilder.string("electronics"))
                                        .build())
                                .build()))
                    .build())
            .build();

    // filtering on regular field works fine
    new LuceneVectorTranslation(
        List.of(
            VectorDataFieldDefinitionBuilder.builder()
                .path(vectorPath)
                .numDimensions(3)
                .similarity(VectorSimilarity.EUCLIDEAN)
                .quantization(VectorQuantization.NONE)
                .build(),
            VectorIndexFilterFieldDefinition.create(filterPath)))
        .translate(query);
  }

  @Test
  public void filterOnAutoEmbedField_throwsException()
      throws
      Exception {
    // Index with TEXT auto-embedding field and separate vector data field
    FieldPath textPath = FieldPath.parse("description");
    FieldPath vectorPath = FieldPath.parse("embedding");

    var query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(vectorPath)
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .filter(
                        new ClauseFilter(
                            ClauseBuilder.simpleClause()
                                .path(textPath) // Filter on auto-embed field
                                .addOperator(
                                    MqlFilterOperatorBuilder.eq()
                                        .value(ValueBuilder.string("laptop"))
                                        .build())
                                .build()))
                    .build())
            .build();

    // Should throw InvalidQueryException
    var exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                new LuceneVectorTranslation(
                    List.of(
                        new VectorTextFieldDefinition("voyage-3-large", textPath),
                        VectorDataFieldDefinitionBuilder.builder()
                            .path(vectorPath)
                            .numDimensions(3)
                            .similarity(VectorSimilarity.EUCLIDEAN)
                            .quantization(VectorQuantization.NONE)
                            .build()))
                    .translate(query));

    assertTrue(
        "Expected error about auto-embedding field, got: " + exception.getMessage(),
        exception.getMessage().contains("is an auto-embedding field"));
    assertTrue(
        "Expected field name in error message, got: " + exception.getMessage(),
        exception.getMessage().contains("description"));
  }
}
