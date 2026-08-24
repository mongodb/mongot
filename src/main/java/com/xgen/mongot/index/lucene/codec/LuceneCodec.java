package com.xgen.mongot.index.lucene.codec;

import static com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat.DEFAULT_HNSW_GRAPH_THRESHOLD;

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
import com.xgen.mongot.index.lucene.vector.HnswWriteParams;
import com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat;
import com.xgen.mongot.index.lucene.vector.NativeHnswScalarQuantizedVectorsFormat;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

/**
 * Customized codec that overrides stored fields, postings, and KNN vector formats on top of a
 * {@link WriteFormat}-selected delegate.
 *
 * <h3>On-disk write formats</h3>
 *
 * <p>The delegate codec is selected by {@link WriteFormat}, fixed per codec instance (the codec
 * name is recorded in segment metadata, so per-segment gating is impossible). The only write format
 * today is {@link WriteFormat#LUCENE99}, which delegates to the fork's writable {@link
 * Lucene99Codec}: every byte written is pinned to the <b>Lucene 9.11.1</b> on-disk format,
 * byte-for-byte identical to what mongot wrote on Lucene 9.11.1 and 10.1.0, preserving binary-swap
 * rollback with no re-index. {@link WriteFormat} exists so a future format (the native Lucene 10.4
 * codec) can be added without rewiring every write-format-dependent parameter.
 *
 * <p>The fork's write-version defaults match upstream 10.4; the 9.11-compatible write path opts
 * down explicitly ({@code backward_codecs.lucene99.Lucene99Codec} internally for points, {@link
 * WriteFormat#getHnswWriteParams()} for the per-field HNSW formats constructed here).
 *
 * <h3>Stored fields</h3>
 *
 * <p>Uses {@link LuceneStoredFieldsFormat} to improve Stored Source query latency by sacrificing
 * some compression improvements introduced in Lucene 8.7. See <a
 * href="https://tinyurl.com/mrx22sav">this</a> for details. Same class under every write format.
 *
 * <h3>Postings</h3>
 *
 * <p>Delegates to {@link HybridPostingsFormat}, which dynamically selects between a {@link
 * MongotBloomFilteringPostingsFormat} and the delegate codec's default format for the {@code _id}
 * field based on a runtime toggle. Search and vector Lucene indexes both use {@link
 * Factory#forIndexWithBloomFilter} so the same dynamic feature flag applies, except writers for
 * auto-embedding indexes pass a supplier that is always {@code false}. See {@link
 * HybridPostingsFormat} for details on the per-segment format selection, metrics, and rollback
 * behavior.
 *
 * <h3>KNN vectors</h3>
 *
 * <p>The KNN per-field formats target Lucene 9.11.1 classes (or mongot's {@code Mongot01042*} forks
 * thereof). {@code KNN_F32_Q7} uses {@link
 * com.xgen.mongot.index.lucene.vector.NativeHnswScalarQuantizedVectorsFormat} (a drop-in,
 * byte-identical replacement for {@code
 * backward_codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat} that accelerates scoring with
 * native SIMD kernels when available), {@code KNN_VECTOR} uses {@code Lucene99HnswVectorsFormat},
 * and the flat scalar format delegates to {@code
 * backward_codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat}. Per-field KNN formats are
 * SPI-resolved by name on read; the format classes are identical under every write format. {@code
 * KNN_F32_Q7} (like {@code KNN_BYTE}) shares its on-disk name with a stock Lucene class, so {@code
 * bazel/java/spi_service_overrides.tsv} strips the stock SPI registration from the packaged binary
 * at stamp time, making the native format the sole, deterministic {@code forName()} result; see
 * {@link com.xgen.mongot.index.lucene.vector.NativeHnswScalarQuantizedVectorsFormat}'s own javadoc
 * for details. Every HNSW format takes the write-format-dependent parameters — the graph write
 * version and the tiny-segment graph threshold — explicitly, grouped and validated as {@link
 * com.xgen.mongot.index.lucene.vector.HnswWriteParams}; see {@link
 * WriteFormat#getHnswWriteParams()}.
 *
 * <h3>Codec name compatibility</h3>
 *
 * <p>Lucene resolves the codec for reads by looking up the name recorded in segment metadata (see
 * {@link SegmentInfos#readCommit(Directory, String)}), so the name written here must match an
 * SPI-discoverable codec on the read side. The recorded name and the per-field postings hook are
 * both derived from the single delegate instance ({@code getName()} and {@code postingsFormat()}),
 * rather than carried separately, so neither can disagree with the bytes the delegate writes.
 */
public class LuceneCodec extends FilterCodec {

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  /** HNSW construction parameters used if not explicitly specified in IndexDefinition. */
  private static final VectorIndexingAlgorithm DEFAULT_INDEXING_ALGORITHM =
      new VectorIndexingAlgorithm.HnswIndexingAlgorithm();

  /** Selects the delegate codec and thus the on-disk format of newly written segments. */
  public enum WriteFormat {
    /** The fork's writable 9.11.1 format ({@code backward_codecs.lucene99.Lucene99Codec}). */
    LUCENE99(
        9,
        new HnswWriteParams(DEFAULT_HNSW_GRAPH_THRESHOLD, Lucene99HnswVectorsFormat.VERSION_START));

    private final int indexCreatedVersionMajor;
    private final HnswWriteParams hnswWriteParams;

    WriteFormat(int indexCreatedVersionMajor, HnswWriteParams hnswWriteParams) {
      this.indexCreatedVersionMajor = indexCreatedVersionMajor;
      this.hnswWriteParams = hnswWriteParams;
    }

    /**
     * The Lucene major version to pass to {@link
     * org.apache.lucene.index.IndexWriterConfig#setIndexCreatedVersionMajor} when creating a new
     * index, decoupling the on-disk format lifecycle from the library upgrade lifecycle.
     *
     * <p>{@code indexCreatedVersionMajor} is an index-wide, immutable property: it is stamped once
     * when the index is created and ignored when appending to an existing index, and no merge
     * (including {@code forceMerge}) ever changes it. {@link #LUCENE99} stamps 9 so new indexes
     * stay openable by 9.11 rollback binaries.
     */
    public int getIndexCreatedVersionMajor() {
      return this.indexCreatedVersionMajor;
    }

    /**
     * The write parameters passed to every HNSW vector format. {@link #LUCENE99} passes threshold 0
     * (always build a graph — Lucene 10.4's graph-skip writes {@code numLevels=0} metadata that
     * 9.11.1/10.1.0 rollback readers NPE on at query time) and {@link
     * Lucene99HnswVectorsFormat#VERSION_START} (the 9.11-readable varint {@code .vem}/{@code .vex}
     * encoding). Always passed explicitly — the fork class defaults match upstream 10.4
     * (group-varint), so mongot never relies on them.
     */
    public HnswWriteParams getHnswWriteParams() {
      return this.hnswWriteParams;
    }
  }

  private final WriteFormat writeFormat;

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
    this(WriteFormat.LUCENE99, Map.of());
  }

  @VisibleForTesting
  public LuceneCodec(Map<FieldPath, VectorFieldSpecification> fieldMap) {
    this(WriteFormat.LUCENE99, fieldMap);
  }

  @VisibleForTesting
  public LuceneCodec(WriteFormat writeFormat) {
    this(writeFormat, Map.of());
  }

  public LuceneCodec(WriteFormat writeFormat, Map<FieldPath, VectorFieldSpecification> fieldMap) {
    this(writeFormat, fieldMap, () -> false, Optional.empty());
  }

  private LuceneCodec(
      WriteFormat writeFormat,
      Map<FieldPath, VectorFieldSpecification> fieldMap,
      BooleanSupplier bloomFilterForIdFieldEnabledSupplier,
      Optional<IndexMetricsUpdater.IndexingMetricsUpdater> indexingMetricsUpdater) {
    this(
        delegateForWriteFormat(writeFormat),
        writeFormat,
        fieldMap,
        bloomFilterForIdFieldEnabledSupplier,
        indexingMetricsUpdater);
  }

  /**
   * The recorded codec name and the per-field postings hook are both derived from the single
   * delegate instance ({@code getName()} and {@code postingsFormat()}), so neither can disagree
   * with the bytes it writes.
   */
  private LuceneCodec(
      Codec delegate,
      WriteFormat writeFormat,
      Map<FieldPath, VectorFieldSpecification> fieldMap,
      BooleanSupplier bloomFilterForIdFieldEnabledSupplier,
      Optional<IndexMetricsUpdater.IndexingMetricsUpdater> indexingMetricsUpdater) {
    super(delegate.getName(), delegate);
    this.writeFormat = writeFormat;
    this.storedFieldsFormat =
        new LuceneStoredFieldsFormat(LuceneStoredFieldsFormat.Mode.BEST_SPEED);
    this.fieldConfigs = new HashMap<>();

    for (FieldPath path : fieldMap.keySet()) {
      VectorFieldSpecification field = fieldMap.get(path);
      this.fieldConfigs.put(path, field.indexingAlgorithm());
    }

    this.postingsFormat =
        new HybridPostingsFormat(
            bloomFilterForIdFieldEnabledSupplier,
            indexingMetricsUpdater,
            asPerFieldPostingsFormat(delegate.postingsFormat()));
  }

  /**
   * Exhaustive without a default: adding a write format fails compilation until it is handled here.
   */
  private static Codec delegateForWriteFormat(WriteFormat writeFormat) {
    return switch (writeFormat) {
      case LUCENE99 -> new Lucene99Codec();
    };
  }

  /**
   * Every default Lucene codec exposes its postings as a {@link PerFieldPostingsFormat}. A delegate
   * that did not would be unreadable regardless: {@link HybridPostingsFormat} writes per-field data
   * that the SPI-resolved delegate must read back on reopen, so fail fast instead of adapting.
   */
  private static PerFieldPostingsFormat asPerFieldPostingsFormat(PostingsFormat format) {
    if (format instanceof PerFieldPostingsFormat perFieldPostingsFormat) {
      return perFieldPostingsFormat;
    }
    throw new IllegalArgumentException(
        String.format("Delegate codec postings format %s is not per-field", format.getName()));
  }

  /** See {@link WriteFormat#getIndexCreatedVersionMajor()}. */
  public int getIndexCreatedVersionMajor() {
    return this.writeFormat.getIndexCreatedVersionMajor();
  }

  @Override
  public PostingsFormat postingsFormat() {
    return this.postingsFormat;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return this.storedFieldsFormat;
  }

  // No pointsFormat() override: the delegate selects the right BKD version natively — the
  // fork's Lucene99Codec passes the 9.11-compatible VERSION_START explicitly to its points
  // writer (the class default matches upstream 10.4).

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
    // Every HNSW format takes the write-format-dependent parameters explicitly; mongot never
    // relies on the (upstream-matching) class defaults.
    HnswWriteParams hnswWriteParams = this.writeFormat.getHnswWriteParams();
    return switch (typeField) {
      // The fork class takes raw ints; unpack the params at this API boundary.
      case FieldName.TypeField.KNN_VECTOR ->
          new Lucene99HnswVectorsFormat(
              options.maxEdges(),
              options.numEdgeCandidates(),
              Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER,
              null,
              hnswWriteParams.tinySegmentsThreshold(),
              hnswWriteParams.graphWriteVersion());
      case FieldName.TypeField.KNN_BYTE ->
          new Lucene99NativeHnswVectorsFormat(
              options.maxEdges(),
              options.numEdgeCandidates(),
              Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER,
              null,
              hnswWriteParams);
      case FieldName.TypeField.KNN_BIT ->
          new Mongot01042HnswBitVectorsFormat(
              options.maxEdges(),
              options.numEdgeCandidates(),
              Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER,
              Optional.empty(),
              hnswWriteParams);
      case FieldName.TypeField.KNN_F32_Q7 ->
          new NativeHnswScalarQuantizedVectorsFormat(
              options.maxEdges(),
              options.numEdgeCandidates(),
              Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER,
              Optional.empty(),
              hnswWriteParams);
      case FieldName.TypeField.KNN_F32_Q1, FieldName.TypeField.KNN_BYTE_Q1 ->
          new Mongot01042HnswBinaryQuantizedVectorsFormat(
              options.maxEdges(),
              options.numEdgeCandidates(),
              Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER,
              Optional.empty(),
              hnswWriteParams);
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
          WriteFormat.LUCENE99, fieldMap, bloomFilterForIdFieldEnabled, indexingMetricsUpdater);
    }
  }
}
