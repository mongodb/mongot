package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.query.aggregation.AggregationContext;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Test;

/**
 * Tests the token-facets-state-cache-backed {@link AggregationContext.CachedGlobalValueLookup} that
 * {@link LuceneAggregationSearchManager} hands to terms aggregations.
 */
public class LuceneAggregationSearchManagerTest {

  private static final String TOKEN_FIELD =
      FieldName.TypeField.TOKEN.getLuceneFieldName(FieldPath.parse("city"), Optional.empty());

  private Directory directory;
  private IndexReader reader;

  @After
  public void tearDown() throws IOException {
    if (this.reader != null) {
      this.reader.close();
    }
    if (this.directory != null) {
      this.directory.close();
    }
  }

  /** Index the given terms one document per segment, so a global OrdinalMap is required. */
  private void buildMultiSegmentIndex(List<String> terms) throws IOException {
    this.directory = new ByteBuffersDirectory();
    try (IndexWriter writer =
        new IndexWriter(
            this.directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (String term : terms) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField(TOKEN_FIELD, new BytesRef(term)));
        writer.addDocument(doc);
        writer.commit();
      }
    }
    this.reader = DirectoryReader.open(this.directory);
  }

  private LuceneIndexSearcher createSearcher(
      boolean enableFacetingOverTokenFields, Optional<Integer> cardinalityLimit)
      throws IOException {
    return LuceneIndexSearcher.create(
        this.reader,
        new QueryCacheProvider.DefaultQueryCacheProvider(),
        Optional.empty(),
        Optional.empty(),
        false,
        enableFacetingOverTokenFields,
        cardinalityLimit);
  }

  @Test
  public void lookupServesCachedOrdinalMapAcrossQueries() throws Exception {
    buildMultiSegmentIndex(List.of("NYC", "LA", "SF"));
    LuceneIndexSearcher searcher = createSearcher(true, Optional.empty());
    AggregationContext.CachedGlobalValueLookup lookup =
        LuceneAggregationSearchManager.cachedGlobalValueLookup(searcher);

    Optional<SortedSetDocValues> first = lookup.lookup(TOKEN_FIELD);
    Optional<SortedSetDocValues> second = lookup.lookup(TOKEN_FIELD);

    assertThat(first).isPresent();
    assertThat(first.get().getValueCount()).isEqualTo(3);
    // The doc-values wrapper is rebuilt per query, but the expensive OrdinalMap is cached and
    // reused across lookups.
    assertThat(first.get()).isInstanceOf(MultiSortedSetDocValues.class);
    assertThat(((MultiSortedSetDocValues) first.get()).mapping)
        .isSameInstanceAs(((MultiSortedSetDocValues) second.get()).mapping);
  }

  @Test
  public void lookupReturnsEmptyWhenCacheDisabled() throws Exception {
    buildMultiSegmentIndex(List.of("NYC", "LA"));
    LuceneIndexSearcher searcher = createSearcher(false, Optional.empty());
    AggregationContext.CachedGlobalValueLookup lookup =
        LuceneAggregationSearchManager.cachedGlobalValueLookup(searcher);

    assertThat(lookup.lookup(TOKEN_FIELD)).isEmpty();
  }

  @Test
  public void lookupReturnsEmptyForMissingField() throws Exception {
    buildMultiSegmentIndex(List.of("NYC"));
    LuceneIndexSearcher searcher = createSearcher(true, Optional.empty());
    AggregationContext.CachedGlobalValueLookup lookup =
        LuceneAggregationSearchManager.cachedGlobalValueLookup(searcher);

    assertThat(lookup.lookup("$type:token/no_such_field")).isEmpty();
  }

  @Test
  public void lookupReturnsEmptyWhenCardinalityLimitExceeded() throws Exception {
    buildMultiSegmentIndex(List.of("NYC", "LA", "SF"));
    LuceneIndexSearcher searcher = createSearcher(true, Optional.of(1));
    AggregationContext.CachedGlobalValueLookup lookup =
        LuceneAggregationSearchManager.cachedGlobalValueLookup(searcher);

    // Aggregations do not enforce the shared-tier facet cardinality limit; the accumulator falls
    // back to building an uncached view.
    assertThat(lookup.lookup(TOKEN_FIELD)).isEmpty();
  }
}
