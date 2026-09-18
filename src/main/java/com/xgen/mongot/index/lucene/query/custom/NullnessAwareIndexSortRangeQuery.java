package com.xgen.mongot.index.lucene.query.custom;

import com.xgen.mongot.index.lucene.field.FieldName;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps an {@link IndexSortSortedNumericDocValuesRangeQuery} so its binary-search fast path can
 * fire even when the index sort's primary field is a {@code $meta/nullness} prefix field rather
 * than the range-queried value field.
 *
 * <h2>Why this class exists</h2>
 *
 * <p>{@code IndexSortSortedNumericDocValuesRangeQuery}'s fast path is gated, internally, on {@code
 * reader.getMetaData().sort().getSort()[0]} matching the queried field by name. That check is
 * {@code private} and reads directly from {@code context.reader()} — there is no public extension
 * point to change which sort field index it consults.
 *
 * <p>For {@code NUMBER_INT64_V2}/{@code DATE_V2} fields, {@code LuceneIndexSortFactory} prepends an
 * auxiliary {@code MqlNullnessSortField} *before* the real value field in the index's actual {@link
 * Sort} array (since nullness can't be encoded into the long space for these two types). So
 * whenever nullness prefixing applies, {@code getSort()[0]} is the nullness field, not the value
 * field — and the fast path never fires for a range query on the value field, even though the value
 * field is, in effect, sorted.
 *
 * <h2>How this class fixes it, without touching Lucene or the query's own logic</h2>
 *
 * <p>Every internal check the wrapped query performs reads {@code context.reader()....} fresh, once
 * per segment, from the {@link LeafReaderContext} handed to {@code scorerSupplier}. This class
 * intercepts that call and substitutes a {@link LeafReaderContext} whose reader delegates
 * everything to the real segment reader <em>except</em> {@link LeafReader#getMetaData()}, which
 * reports the real index sort with every {@code $meta/nullness} entry dropped ({@link
 * FieldName#isNullnessFieldName(String)}) — i.e. exactly what the index sort would look like if the
 * value field were its own primary sort field.
 */
public final class NullnessAwareIndexSortRangeQuery extends Query {

  private final IndexSortSortedNumericDocValuesRangeQuery delegate;

  public NullnessAwareIndexSortRangeQuery(IndexSortSortedNumericDocValuesRangeQuery delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = this.delegate.rewrite(indexSearcher);
    if (rewritten == this.delegate) {
      return this;
    }
    // The delegate can rewrite itself into a different query type entirely (e.g. a
    // FieldExistsQuery or MatchAllDocsQuery for unbounded ranges). Those don't consult index sort
    // metadata at all, so there's nothing left for this wrapper to do; only re-wrap if it rewrote
    // into another instance of the same class.
    if (rewritten instanceof IndexSortSortedNumericDocValuesRangeQuery rewrittenDelegate) {
      return new NullnessAwareIndexSortRangeQuery(rewrittenDelegate);
    }
    return rewritten;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight realWeight = this.delegate.createWeight(searcher, scoreMode, boost);
    return new FilterWeight(realWeight) {
      @Override
      @Nullable
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader wrapped = new NullnessDroppingLeafReader(context.reader());
        return realWeight.scorerSupplier(wrapped.getContext());
      }
    };
  }

  @Override
  public String toString(String field) {
    return "NullnessAwareIndexSortRangeQuery(" + this.delegate.toString(field) + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    this.delegate.visit(visitor);
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj)
        && this.delegate.equals(((NullnessAwareIndexSortRangeQuery) obj).delegate);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + this.delegate.hashCode();
  }

  /**
   * A {@link LeafReader} that delegates all data access to the wrapped reader unchanged, except
   * {@link #getMetaData()}, which reports the real index sort with every {@code $meta/nullness}
   * entry dropped. See the class-level javadoc of {@link NullnessAwareIndexSortRangeQuery} for why
   * this is a faithful, not lossy, description of the real physical document order.
   */
  private static final class NullnessDroppingLeafReader extends FilterLeafReader {

    NullnessDroppingLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public LeafMetaData getMetaData() {
      LeafMetaData real = this.in.getMetaData();
      Sort realSort = Objects.requireNonNull(real.sort(), "index sort");
      SortField[] filtered =
          Arrays.stream(realSort.getSort())
              .filter(sortField -> !FieldName.isNullnessFieldName(sortField.getField()))
              .toArray(SortField[]::new);
      return new LeafMetaData(
          real.createdVersionMajor(), real.minVersion(), new Sort(filtered), real.hasBlocks());
    }

    // Both cache helpers return null: this is a short-lived, per-call wrapper that alters
    // getMetaData(), matching the documented null-return case on both CacheHelper javadocs
    // ("short-lived wrappers that alter the content of the wrapped reader"). No caching benefit is
    // lost by doing so - this reader is constructed fresh per scorerSupplier() call and never
    // reused across segments or searches, so there's nothing worth caching against its identity.
    @Override
    @Nullable
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    @Override
    @Nullable
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }
}
