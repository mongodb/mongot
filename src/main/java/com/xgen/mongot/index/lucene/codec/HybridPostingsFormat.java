package com.xgen.mongot.index.lucene.codec;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * Per-field postings format that switches the {@code _id} field between bloom and non-bloom
 * encodings based on a runtime flag.
 *
 * <p>For {@link FieldName.MetaField#ID}, {@link #getPostingsFormatForField(String)} returns either:
 *
 * <ul>
 *   <li>{@link MongotBloomFilteringPostingsFormat}, wrapping the same Lucene99 delegate postings
 *       format that {@link Lucene99Codec#getPostingsFormatForField(String)} uses for {@code _id},
 *       when the supplier returns {@code true}; or
 *   <li>that delegate format directly when the supplier returns {@code false}.
 * </ul>
 *
 * <p>All other fields use {@link Lucene99Codec#getPostingsFormatForField(String)} unchanged. The
 * supplier is read whenever Lucene resolves the postings format for {@code _id} while building a
 * segment (for example on flush or merge), so adjacent segments can record different choices in
 * field metadata.
 *
 * @see LuceneCodec
 */
class HybridPostingsFormat extends PerFieldPostingsFormat {

  private static final String ID_FIELD_NAME = FieldName.MetaField.ID.getLuceneFieldName();

  private final Lucene99Codec delegateCodec;
  private final PostingsFormat defaultIdPostingsFormat;
  private final PostingsFormat bloomIdPostingsFormat;
  private final BooleanSupplier bloomFilterForIdFieldEnabledSupplier;
  private final Optional<IndexMetricsUpdater.IndexingMetricsUpdater> indexingMetricsUpdater;

  public HybridPostingsFormat(
      BooleanSupplier bloomFilterForIdFieldEnabledSupplier,
      Optional<IndexMetricsUpdater.IndexingMetricsUpdater> indexingMetricsUpdater,
      Lucene99Codec delegateCodec) {

    PostingsFormat defaultIdPostingsFormat = delegateCodec.getPostingsFormatForField(ID_FIELD_NAME);

    this.bloomFilterForIdFieldEnabledSupplier = bloomFilterForIdFieldEnabledSupplier;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
    this.defaultIdPostingsFormat = defaultIdPostingsFormat;
    this.bloomIdPostingsFormat = new MongotBloomFilteringPostingsFormat(defaultIdPostingsFormat);
    this.delegateCodec = delegateCodec;
  }

  @Override
  public PostingsFormat getPostingsFormatForField(String field) {
    if (field.equals(ID_FIELD_NAME)) {
      if (this.bloomFilterForIdFieldEnabledSupplier.getAsBoolean()) {
        this.indexingMetricsUpdater.ifPresent(
            u -> u.getBloomFilterIdPostingCreatedCounter().increment());
        return this.bloomIdPostingsFormat;
      } else {
        this.indexingMetricsUpdater.ifPresent(
            u -> u.getLucene99IdPostingCreatedCounter().increment());
        return this.defaultIdPostingsFormat;
      }
    } else {
      return this.delegateCodec.getPostingsFormatForField(field);
    }
  }
}
