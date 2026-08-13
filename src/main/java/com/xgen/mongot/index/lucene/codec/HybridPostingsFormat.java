package com.xgen.mongot.index.lucene.codec;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * Per-field postings format that switches the {@code _id} field between bloom and non-bloom
 * encodings based on a runtime flag.
 *
 * <p>For {@link FieldName.MetaField#ID}, {@link #getPostingsFormatForField(String)} returns either:
 *
 * <ul>
 *   <li>{@link MongotBloomFilteringPostingsFormat}, wrapping the same delegate postings format that
 *       the delegate codec's {@code getPostingsFormatForField} uses for {@code _id}, when the
 *       supplier returns {@code true}; or
 *   <li>that delegate format directly when the supplier returns {@code false}.
 * </ul>
 *
 * <p>All other fields use the delegate codec's per-field postings format unchanged. The delegate is
 * the delegate codec's own {@code postingsFormat()}, a {@link PerFieldPostingsFormat}. The supplier
 * is read whenever Lucene resolves the postings format for {@code _id} while building a segment
 * (for example on flush or merge), so adjacent segments can record different choices in field
 * metadata.
 *
 * @see LuceneCodec
 */
class HybridPostingsFormat extends PerFieldPostingsFormat {

  private static final String ID_FIELD_NAME = FieldName.MetaField.ID.getLuceneFieldName();

  private final PerFieldPostingsFormat delegatePostingsFormat;
  private final PostingsFormat defaultIdPostingsFormat;
  private final PostingsFormat bloomIdPostingsFormat;
  private final BooleanSupplier bloomFilterForIdFieldEnabledSupplier;
  private final Optional<IndexMetricsUpdater.IndexingMetricsUpdater> indexingMetricsUpdater;

  public HybridPostingsFormat(
      BooleanSupplier bloomFilterForIdFieldEnabledSupplier,
      Optional<IndexMetricsUpdater.IndexingMetricsUpdater> indexingMetricsUpdater,
      PerFieldPostingsFormat delegatePostingsFormat) {

    PostingsFormat defaultIdPostingsFormat =
        delegatePostingsFormat.getPostingsFormatForField(ID_FIELD_NAME);

    this.bloomFilterForIdFieldEnabledSupplier = bloomFilterForIdFieldEnabledSupplier;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
    this.defaultIdPostingsFormat = defaultIdPostingsFormat;
    this.bloomIdPostingsFormat = new MongotBloomFilteringPostingsFormat(defaultIdPostingsFormat);
    this.delegatePostingsFormat = delegatePostingsFormat;
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
      return this.delegatePostingsFormat.getPostingsFormatForField(field);
    }
  }
}
