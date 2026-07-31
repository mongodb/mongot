package com.xgen.mongot.index.lucene.codec;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat;
import com.xgen.mongot.index.lucene.codec.flat.BinaryQuantizedFlatVectorsFormat;
import com.xgen.mongot.index.lucene.codec.flat.FlatBitVectorsFormat;
import com.xgen.mongot.index.lucene.codec.flat.Float32AndByteFlatVectorsFormat;
import com.xgen.mongot.index.lucene.codec.flat.ScalarQuantizedFlatVectorsFormat;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBinaryQuantizedVectorsFormat;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBitVectorsFormat;
import com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

/**
 * Customized {@link Lucene99Codec} that overrides stored fields, postings, and KNN vector formats.
 *
 * <h3>Stored fields</h3>
 *
 * <p>Uses {@link LuceneStoredFieldsFormat} to improve Stored Source query latency by sacrificing
 * some compression improvements introduced in Lucene 8.7. See <a
 * href="https://tinyurl.com/mrx22sav">this</a> for details.
 *
 * <h3>Postings</h3>
 *
 * <p>Delegates to {@link HybridPostingsFormat}, which dynamically selects between a {@link
 * MongotBloomFilteringPostingsFormat} and the default Lucene99 format for the {@code _id} field
 * based on a runtime toggle. Search and vector Lucene indexes both use {@link
 * Factory#forIndexWithBloomFilter} so the same dynamic feature flag applies, except writers for
 * auto-embedding indexes pass a supplier that is always {@code false}. See {@link
 * HybridPostingsFormat} for details on the per-segment format selection, metrics, and rollback
 * behavior.
 *
 * <h3>Codec name compatibility</h3>
 *
 * <p>The codec name is kept as "Lucene99" so that Lucene resolves the original codec for reads (the
 * name is looked up from segment metadata in {@link SegmentInfos#readCommit(Directory, String)}).
 * This guarantees that our customizations are compatible with the original codec and no re-indexing
 * is required to upgrade to the next major Lucene version.
 */
public class LuceneCodec extends FilterCodec {

  /**
   * The Lucene major version used when creating new index segments. This is passed to {@link
   * org.apache.lucene.index.IndexWriterConfig#setIndexCreatedVersionMajor} to decouple the codec
   * upgrade lifecycle from the Lucene library upgrade lifecycle.
   */
  public static final int CODEC_VERSION_MAJOR = 9;

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  /** HNSW construction parameters used if not explicitly specified in IndexDefinition. */
  private static final VectorIndexingAlgorithm DEFAULT_INDEXING_ALGORITHM =
      new VectorIndexingAlgorithm.HnswIndexingAlgorithm();

  private static final String CODEC_NAME = "Lucene99";

  private final PostingsFormat postingsFormat;

  private final StoredFieldsFormat storedFieldsFormat;

  private final KnnVectorsFormat knnVectorsFormat =
      new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          return LuceneCodec.this.getKnnVectorsFormatForField(field);
        }

        @Override
        public int getMaxDimensions(String fieldName) {
          return VectorFieldSpecification.MAX_DIMENSIONS;
        }
      };

  private final Map<FieldPath, VectorIndexingAlgorithm> fieldConfigs;

  @VisibleForTesting
  public LuceneCodec() {
    this(CODEC_NAME, Map.of());
  }

  @VisibleForTesting
  public LuceneCodec(String codecName) {
    this(codecName, Map.of());
  }

  @VisibleForTesting
  public LuceneCodec(Map<FieldPath, VectorFieldSpecification> fieldMap) {
    this(CODEC_NAME, fieldMap);
  }

  private LuceneCodec(String codecName, Map<FieldPath, VectorFieldSpecification> fieldMap) {
    this(codecName, fieldMap, new Lucene99Codec(), () -> false, Optional.empty());
  }

  private LuceneCodec(
      String codecName,
      Map<FieldPath, VectorFieldSpecification> fieldMap,
      Lucene99Codec delegateCodec,
      BooleanSupplier bloomFilterForIdFieldEnabledSupplier,
      Optional<IndexMetricsUpdater.IndexingMetricsUpdater> indexingMetricsUpdater) {
    super(codecName, delegateCodec);
    this.storedFieldsFormat =
        new LuceneStoredFieldsFormat(LuceneStoredFieldsFormat.Mode.BEST_SPEED);
    this.fieldConfigs = new HashMap<>();

    for (FieldPath path : fieldMap.keySet()) {
      VectorFieldSpecification field = fieldMap.get(path);
      this.fieldConfigs.put(path, field.indexingAlgorithm());
    }
    this.postingsFormat =
        new HybridPostingsFormat(
            bloomFilterForIdFieldEnabledSupplier, indexingMetricsUpdater, delegateCodec);
  }

  @Override
  public PostingsFormat postingsFormat() {
    return this.postingsFormat;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return this.storedFieldsFormat;
  }

  @Override
  public final KnnVectorsFormat knnVectorsFormat() {
    return this.knnVectorsFormat;
  }

  private KnnVectorsFormat getKnnVectorsFormatForField(String field) {
    FieldName.TypeField typeField =
        Check.isPresent(FieldName.TypeField.getTypeOf(field), "typeField");

    VectorIndexingAlgorithm algorithm = getKnnFieldConfig(typeField, field);

    return switch (algorithm) {
      case VectorIndexingAlgorithm.HnswIndexingAlgorithm hnswIndexingAlgorithm ->
          resolveHnswVectorFormat(field, typeField, hnswIndexingAlgorithm.options());
      case VectorIndexingAlgorithm.FlatIndexingAlgorithm flatIndexingAlgorithm ->
          resolveFlatVectorFormat(field, typeField);
    };
  }

  private KnnVectorsFormat resolveHnswVectorFormat(
      String field, FieldName.TypeField typeField, VectorFieldSpecification.HnswOptions options) {
    return switch (typeField) {
      case FieldName.TypeField.KNN_VECTOR ->
          new Lucene99HnswVectorsFormat(options.maxEdges(), options.numEdgeCandidates());
      case FieldName.TypeField.KNN_BYTE ->
          new Lucene99NativeHnswVectorsFormat(options.maxEdges(), options.numEdgeCandidates());
      case FieldName.TypeField.KNN_BIT ->
          new Mongot01042HnswBitVectorsFormat(options.maxEdges(), options.numEdgeCandidates());
      case FieldName.TypeField.KNN_F32_Q7 ->
          new Lucene99HnswScalarQuantizedVectorsFormat(
              options.maxEdges(),
              options.numEdgeCandidates(),
              DEFAULT_NUM_MERGE_WORKER,
              7,
              false,
              null,
              null);
      case FieldName.TypeField.KNN_F32_Q1, FieldName.TypeField.KNN_BYTE_Q1 ->
          new Mongot01042HnswBinaryQuantizedVectorsFormat(
              options.maxEdges(), options.numEdgeCandidates());
      default -> {
        FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
            "Unexpected field type %s for KNN field %s. Using default", typeField, field);
        throw new IllegalStateException(String.format("Unexpected field type %s", typeField));
      }
    };
  }

  private KnnVectorsFormat resolveFlatVectorFormat(String field, FieldName.TypeField typeField) {
    return switch (typeField) {
      // TODO(CLOUDP-403218): extend native scoring support to flat indexes.
      case FieldName.TypeField.KNN_VECTOR, FieldName.TypeField.KNN_BYTE ->
          new Float32AndByteFlatVectorsFormat();
      case FieldName.TypeField.KNN_BIT -> new FlatBitVectorsFormat();
      case FieldName.TypeField.KNN_F32_Q7 -> new ScalarQuantizedFlatVectorsFormat(null, 7, false);
      case FieldName.TypeField.KNN_F32_Q1, FieldName.TypeField.KNN_BYTE_Q1 ->
          new BinaryQuantizedFlatVectorsFormat();
      default -> {
        FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
            "Unexpected field type %s for KNN field %s. Using default", typeField, field);
        throw new IllegalStateException(String.format("Unexpected field type %s", typeField));
      }
    };
  }

  private VectorIndexingAlgorithm getKnnFieldConfig(
      FieldName.TypeField typeField, String luceneField) {
    FieldPath fieldPath = FieldPath.parse(typeField.stripPrefix(luceneField));
    return this.fieldConfigs.getOrDefault(fieldPath, DEFAULT_INDEXING_ALGORITHM);
  }

  public static class Factory {

    public static LuceneCodec forIndexWithBloomFilter(
        Map<FieldPath, VectorFieldSpecification> fieldMap,
        BooleanSupplier bloomFilterForIdFieldEnabled,
        Optional<IndexMetricsUpdater.IndexingMetricsUpdater> indexingMetricsUpdater) {
      return new LuceneCodec(
          CODEC_NAME,
          fieldMap,
          new Lucene99Codec(),
          bloomFilterForIdFieldEnabled,
          indexingMetricsUpdater);
    }
  }
}
