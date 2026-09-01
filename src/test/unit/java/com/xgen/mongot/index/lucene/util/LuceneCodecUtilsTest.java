package com.xgen.mongot.index.lucene.util;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import org.junit.Test;

public class LuceneCodecUtilsTest {

  @Test
  public void extractVectorFieldsFromSearchMappings_findsNestedVectors() {
    VectorFieldSpecification topLevel = vectorSpecification(16, 100);
    VectorFieldSpecification nestedDocument = vectorSpecification(32, 200);
    VectorFieldSpecification nestedEmbeddedDocument = vectorSpecification(48, 300);
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "top",
                FieldDefinitionBuilder.builder()
                    .searchIndexVectorField(new SearchIndexVectorFieldDefinition(topLevel))
                    .build())
            .field(
                "parent",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "nested",
                                FieldDefinitionBuilder.builder()
                                    .searchIndexVectorField(
                                        new SearchIndexVectorFieldDefinition(nestedDocument))
                                    .build())
                            .build())
                    .build())
            .field(
                "items",
                FieldDefinitionBuilder.builder()
                    .embeddedDocuments(
                        EmbeddedDocumentsFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "vector",
                                FieldDefinitionBuilder.builder()
                                    .searchIndexVectorField(
                                        new SearchIndexVectorFieldDefinition(
                                            nestedEmbeddedDocument))
                                    .build())
                            .build())
                    .build())
            .build();

    assertThat(LuceneCodecUtils.extractVectorFieldsFromSearchMappings(mappings))
        .containsExactly(
            FieldPath.parse("top"),
            topLevel,
            FieldPath.parse("parent.nested"),
            nestedDocument,
            FieldPath.parse("items.vector"),
            nestedEmbeddedDocument);
  }

  private static VectorFieldSpecification vectorSpecification(int maxEdges, int numEdgeCandidates) {
    return new VectorFieldSpecification(
        512,
        VectorSimilarity.COSINE,
        VectorQuantization.NONE,
        new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
            new VectorFieldSpecification.HnswOptions(maxEdges, numEdgeCandidates)));
  }
}
