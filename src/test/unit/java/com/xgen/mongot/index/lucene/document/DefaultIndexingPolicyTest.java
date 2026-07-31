package com.xgen.mongot.index.lucene.document;

import static com.xgen.mongot.index.definition.StoredSourceDefinition.Mode.INCLUSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.ingestion.BsonDocumentProcessor;
import com.xgen.mongot.index.lucene.document.block.VectorEmbeddedDocumentBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBlockBuilder;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

/** Unit tests for {@link DefaultIndexingPolicy} and nested vector indexing behavior. */
public class DefaultIndexingPolicyTest {

  private static final byte[] DUMMY_ID = LuceneDocumentIdEncoder.encodeDocumentId(new BsonInt32(1));
  private static final IndexingMetricsUpdater METRICS_UPDATER =
      new IndexingMetricsUpdater(
          SearchIndex.mockMetricsFactory(),
          com.xgen.mongot.index.definition.IndexDefinition.Type.VECTOR_SEARCH);

  @Test
  public void create_vectorIndexWithoutNestedRoot_returnsPolicyThatProducesSingleDocument()
      throws IOException {
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withCosineVectorField("embedding", 2)
            .withFilterPath("name")
            .build();

    LuceneIndexingPolicy policy =
        DefaultIndexingPolicy.create(
            indexDefinition,
            indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT),
            METRICS_UPDATER,
            FeatureFlags.getDefault());

    DocumentBlockBuilder builder = policy.createBuilder(DUMMY_ID);
    BsonDocument doc =
        new BsonDocument()
            .append("_id", new BsonInt32(1))
            .append("embedding", new BsonArray(List.of(new BsonDouble(1.0), new BsonDouble(2.0))))
            .append("name", new BsonString("a"));
    BsonDocumentProcessor.process(BsonUtils.documentToRaw(doc), builder);

    List<Document> block = builder.buildBlock();
    assertEquals(
        "Vector index without nestedRoot should produce exactly one Lucene document per source doc",
        1,
        block.size());
  }

  @Test
  public void create_vectorIndexWithNestedRoot_returnsPolicyThatProducesBlockBuilder()
      throws IOException {
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .nestedRoot("sections")
            .withCosineVectorField("sections.embedding", 2)
            .withFilterPath("sections.name")
            .build();

    LuceneIndexingPolicy policy =
        DefaultIndexingPolicy.create(
            indexDefinition,
            indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT),
            METRICS_UPDATER,
            FeatureFlags.getDefault());

    DocumentBlockBuilder builder = policy.createBuilder(DUMMY_ID);
    BsonDocument doc =
        new BsonDocument()
            .append("_id", new BsonInt32(1))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append(
                                "embedding",
                                new BsonArray(List.of(new BsonDouble(1.0), new BsonDouble(2.0))))
                            .append("name", new BsonString("a")),
                        new BsonDocument()
                            .append(
                                "embedding",
                                new BsonArray(List.of(new BsonDouble(3.0), new BsonDouble(4.0))))
                            .append("name", new BsonString("b")))));
    BsonDocumentProcessor.process(BsonUtils.documentToRaw(doc), builder);

    List<Document> block = builder.buildBlock();
    assertTrue(
        "Vector index with nestedRoot and array should produce "
            + "multiple Lucene documents (root + one per array element)",
        block.size() >= 2);
    assertEquals("Expected 1 root + 2 embedded docs for 2 array elements", 3, block.size());
  }

  @Test
  public void create_withNestedRootEmptyArray_producesSingleDocument() throws IOException {
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .nestedRoot("sections")
            .withCosineVectorField("sections.embedding", 2)
            .withFilterPath("sections.name")
            .build();

    LuceneIndexingPolicy policy =
        DefaultIndexingPolicy.create(
            indexDefinition,
            indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT),
            METRICS_UPDATER,
            FeatureFlags.getDefault());

    DocumentBlockBuilder builder = policy.createBuilder(DUMMY_ID);
    BsonDocument doc =
        new BsonDocument().append("_id", new BsonInt32(1)).append("sections", new BsonArray());
    BsonDocumentProcessor.process(BsonUtils.documentToRaw(doc), builder);

    List<Document> block = builder.buildBlock();
    assertEquals(
        "Vector index with nestedRoot but empty array should produce only the root document",
        1,
        block.size());
  }

  @Test
  public void create_withNestedRootAndRootLevelFields_producesBlockWithRootAndEmbeddedDocs()
      throws IOException {
    // Index has both root-level vector/filter and nested (sections) vector/filter
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .nestedRoot("sections")
            .withCosineVectorField("doc_embedding", 2)
            .withFilterPath("title")
            .withCosineVectorField("sections.embedding", 2)
            .withFilterPath("sections.name")
            .build();

    LuceneIndexingPolicy policy =
        DefaultIndexingPolicy.create(
            indexDefinition,
            indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT),
            METRICS_UPDATER,
            FeatureFlags.getDefault());

    DocumentBlockBuilder builder = policy.createBuilder(DUMMY_ID);
    BsonDocument doc =
        new BsonDocument()
            .append("_id", new BsonInt32(1))
            .append(
                "doc_embedding", new BsonArray(List.of(new BsonDouble(0.5), new BsonDouble(0.5))))
            .append("title", new BsonString("root doc"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append(
                                "embedding",
                                new BsonArray(List.of(new BsonDouble(1.0), new BsonDouble(2.0))))
                            .append("name", new BsonString("a")),
                        new BsonDocument()
                            .append(
                                "embedding",
                                new BsonArray(List.of(new BsonDouble(3.0), new BsonDouble(4.0))))
                            .append("name", new BsonString("b")))));
    BsonDocumentProcessor.process(BsonUtils.documentToRaw(doc), builder);

    List<Document> block = builder.buildBlock();
    assertTrue(
        "Mixed root + nested: should produce "
            + "root doc (with doc_embedding, title) + one doc per section",
        block.size() >= 2);
    assertEquals(
        "Expected 1 root (with root-level fields) + 2 embedded docs for 2 section elements",
        3,
        block.size());
  }

  @Test
  public void create_vectorIndexWithDeepNestedRoot_producesBlockWithRootAndEmbeddedDocs()
      throws IOException {
    // Nested root at multi-segment path: chapters.sections.
    // The intermediate ancestor "chapters" must be traversed with embedded-root awareness so that
    // the nestedRoot check fires at "chapters.sections" and child documents are created correctly.
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .nestedRoot("chapters.sections")
            .withCosineVectorField("chapters.sections.embedding", 2)
            .withFilterPath("chapters.sections.name")
            .build();

    LuceneIndexingPolicy policy =
        DefaultIndexingPolicy.create(
            indexDefinition,
            indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT),
            METRICS_UPDATER,
            FeatureFlags.getDefault());

    DocumentBlockBuilder builder = policy.createBuilder(DUMMY_ID);
    assertTrue(
        "Nested root at deep path should still use VectorEmbeddedDocumentBuilder (embedded path)",
        builder instanceof VectorEmbeddedDocumentBuilder);

    BsonDocument doc =
        new BsonDocument()
            .append("_id", new BsonInt32(1))
            .append(
                "chapters",
                new BsonDocument()
                    .append(
                        "sections",
                        new BsonArray(
                            List.of(
                                new BsonDocument()
                                    .append(
                                        "embedding",
                                        new BsonArray(
                                            List.of(new BsonDouble(1.0), new BsonDouble(2.0))))
                                    .append("name", new BsonString("a")),
                                new BsonDocument()
                                    .append(
                                        "embedding",
                                        new BsonArray(
                                            List.of(new BsonDouble(3.0), new BsonDouble(4.0))))
                                    .append("name", new BsonString("b"))))));
    BsonDocumentProcessor.process(BsonUtils.documentToRaw(doc), builder);

    List<Document> block = builder.buildBlock();
    assertEquals(
        "Expected 1 root document + 2 embedded child documents for 2 sections elements",
        3,
        block.size());
  }

  @Test
  public void create_vectorIndexWithNestedRootAndStoredSource_rootDocumentHasStoredSourceField()
      throws IOException {
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .nestedRoot("sections")
            .withCosineVectorField("sections.embedding", 2)
            .withFilterPath("sections.name")
            .storedSource(StoredSourceDefinition.create(INCLUSION, List.of("_id", "title")))
            .build();

    LuceneIndexingPolicy policy =
        DefaultIndexingPolicy.create(
            indexDefinition,
            indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT),
            METRICS_UPDATER,
            FeatureFlags.getDefault());

    DocumentBlockBuilder builder = policy.createBuilder(DUMMY_ID);
    BsonDocument doc =
        new BsonDocument()
            .append("_id", new BsonInt32(1))
            .append("title", new BsonString("root doc"))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append(
                                "embedding",
                                new BsonArray(List.of(new BsonDouble(1.0), new BsonDouble(2.0))))
                            .append("name", new BsonString("a")),
                        new BsonDocument()
                            .append(
                                "embedding",
                                new BsonArray(List.of(new BsonDouble(3.0), new BsonDouble(4.0))))
                            .append("name", new BsonString("b")))));
    BsonDocumentProcessor.process(BsonUtils.documentToRaw(doc), builder);

    List<Document> block = builder.buildBlock();
    assertEquals("Expected 1 root + 2 embedded docs for 2 section elements", 3, block.size());

    // Root document is built last in DocumentBlock.build() (children first, then buildRoot())
    Document rootDocument = block.get(block.size() - 1);
    String storedSourceFieldName = FieldName.StaticField.STORED_SOURCE.getLuceneFieldName();
    assertNotNull(
        "Root document should have stored source field when index has storedSource and nestedRoot "
            + "(RootDocumentBuilder must wrap the root)",
        rootDocument.getField(storedSourceFieldName));
  }

  @Test
  public void create_searchIndexWithByteBinaryVector_flagEnabled_indexesVector()
      throws IOException {
    FeatureFlags enabledFlags =
        FeatureFlags.withDefaults().enable(Feature.BYTE_BINARY_QUANTIZATION).build();

    Document doc = buildSearchIndexDocumentWithByteBinaryVector(enabledFlags);

    String expectedVectorFieldName =
        FieldName.TypeField.KNN_BYTE_Q1.getLuceneFieldName(
            FieldPath.newRoot("vector"), java.util.Optional.empty());
    assertNotNull(
        "Injected enabled FeatureFlags should allow BYTE+BINARY vector indexing on search path",
        doc.getField(expectedVectorFieldName));
  }

  @Test
  public void create_searchIndexWithByteBinaryVector_flagDisabled_skipsVector() throws IOException {
    FeatureFlags disabledFlags = FeatureFlags.withDefaults().build();

    Document doc = buildSearchIndexDocumentWithByteBinaryVector(disabledFlags);

    String expectedVectorFieldName =
        FieldName.TypeField.KNN_BYTE_Q1.getLuceneFieldName(
            FieldPath.newRoot("vector"), java.util.Optional.empty());
    assertNull(
        "Injected disabled FeatureFlags should skip BYTE+BINARY vector indexing on search path",
        doc.getField(expectedVectorFieldName));
  }

  private static Document buildSearchIndexDocumentWithByteBinaryVector(FeatureFlags featureFlags)
      throws IOException {
    var vectorSpec =
        new VectorFieldSpecification(
            2,
            VectorSimilarity.COSINE,
            VectorQuantization.BINARY,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm());
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "vector",
                FieldDefinitionBuilder.builder()
                    .searchIndexVectorField(new SearchIndexVectorFieldDefinition(vectorSpec))
                    .build())
            .build();
    var indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings)
            .indexFeatureVersion(
                com.xgen.mongot.index.definition.SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .build();

    LuceneIndexingPolicy policy =
        DefaultIndexingPolicy.create(
            new KeywordAnalyzer(),
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            featureFlags);

    DocumentBlockBuilder builder = policy.createBuilder(DUMMY_ID);
    Vector byteVector = Vector.fromBytes(new byte[] {(byte) 0x00, (byte) 0xFF});
    BsonDocument sourceDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(1))
            .append("vector", BsonVectorParser.encode(byteVector));
    BsonDocumentProcessor.process(BsonUtils.documentToRaw(sourceDocument), builder);

    List<Document> block = builder.buildBlock();
    return block.getFirst();
  }
}
