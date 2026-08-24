package com.xgen.mongot.index.lucene.vector;

import static com.google.common.truth.Truth.assertWithMessage;
import static com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat.DEFAULT_HNSW_GRAPH_THRESHOLD;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;

import com.google.common.truth.Truth;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Runs Lucene's standard {@link BaseKnnVectorsFormatTestCase} suite against {@link
 * NativeHnswScalarQuantizedVectorsFormat} with native scoring forced on. Float vectors are
 * quantized to Q7 and scored by the native scalar path; byte vectors pass through to the (delegate)
 * raw format. This validates the full add/merge/search lifecycle and on-disk round-trip.
 *
 * <p>The native scorer reads the {@code NATIVE_SCALAR_VECTOR_SCORING} flag from the process-global
 * {@link DynamicFeatureFlagRegistry}, so the registry is installed once (with the flag enabled)
 * before any test method runs. Bazel runs each test class in its own JVM, so initializing the
 * process-global instance here does not collide with other test classes.
 */
public class NativeHnswScalarQuantizedVectorsFormatTest extends BaseKnnVectorsFormatTestCase {

  @BeforeClass
  public static void enableNativeScalarScoring() {
    // Reset first so this is robust if another test class already installed the process-global
    // instance in a shared JVM (initializeProcessInstance throws if one is already present).
    DynamicFeatureFlagRegistry.resetForTest();
    DynamicFeatureFlagConfig enabled =
        new DynamicFeatureFlagConfig(
            DynamicFeatureFlags.NATIVE_SCALAR_VECTOR_SCORING.getName(),
            DynamicFeatureFlagConfig.Phase.ENABLED,
            List.of(),
            List.of(),
            100,
            DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);
    DynamicFeatureFlagRegistry.initializeProcessInstance(
        new DynamicFeatureFlagRegistry(
            Optional.of(List.of(enabled)),
            Optional.empty(),
            Optional.empty(),
            Optional.of(new ObjectId())));
  }

  @AfterClass
  public static void resetRegistry() {
    DynamicFeatureFlagRegistry.resetForTest();
  }

  // Matches the fork's TestLucene99HnswScalarQuantizedVectorsFormat: the 9.9-era scalar-quantized
  // reader does not serve dequantized floats when the raw vector data is missing.
  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new NativeHnswScalarQuantizedVectorsFormat();
      }
    };
  }

  @Test
  public void constructor_zeroMergeWorkers_throws() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            new NativeHnswScalarQuantizedVectorsFormat(
                DEFAULT_MAX_CONN,
                DEFAULT_BEAM_WIDTH,
                0,
                Optional.empty(),
                new HnswWriteParams(
                    DEFAULT_HNSW_GRAPH_THRESHOLD, Lucene99HnswVectorsFormat.VERSION_START)));
  }

  @Test
  public void constructor_negativeMergeWorkers_throws() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            new NativeHnswScalarQuantizedVectorsFormat(
                DEFAULT_MAX_CONN,
                DEFAULT_BEAM_WIDTH,
                -1,
                Optional.empty(),
                new HnswWriteParams(
                    DEFAULT_HNSW_GRAPH_THRESHOLD, Lucene99HnswVectorsFormat.VERSION_START)));
  }

  /**
   * Tiny segments (below Lucene 10.4's {@code HNSW_GRAPH_THRESHOLD}) must still get a real HNSW
   * graph -- graph-less ones write {@code numLevels=0}, which pre-10.4 readers (mongot on 9.11.1 /
   * 10.1.0) NPE on. Mirrors {@code
   * MongotHnswScalarQuantizedVectorsFormatTest#fieldsWriter_tinySegmentFlush_writesHnswGraph}.
   */
  @Test
  public void fieldsWriter_tinySegmentFlush_writesHnswGraph() throws IOException {
    String field = "field";
    // A plain IndexWriterConfig (no framework randomization) so all docs flush as one segment.
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(getCodec()))) {
      int numDocs = 8;
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(field, randomVector(16), VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        Truth.assertThat(reader.leaves()).hasSize(1);
        LeafReader leaf = reader.leaves().get(0).reader();
        KnnVectorsReader fieldReader =
            ((PerFieldKnnVectorsFormat.FieldsReader) ((CodecReader) leaf).getVectorReader())
                .getFieldReader(field);
        Truth.assertThat(fieldReader).isInstanceOf(HnswGraphProvider.class);
        HnswGraph graph = ((HnswGraphProvider) fieldReader).getGraph(field);
        assertWithMessage(
                "expected an HNSW graph on a tiny (%s-vector) segment; graph-less segments break"
                    + " 9.11/10.1 rollback",
                numDocs)
            .that(graph.numLevels())
            .isAtLeast(1);
      }
    }
  }
}
