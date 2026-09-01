package com.xgen.mongot.index.lucene.facet;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.testing.FakeTicker;
import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;

public class TokensFacetStateCacheTest {

  @Test
  public void testSimpleLoad() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedDocuments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var cache = TokenFacetsStateCache.create(reader, Optional.empty());
      Optional<TokenSsdvFacetState> state = cache.get("foo");
      assertThat(state).isPresent();
      assertThat(state.get().getField()).isEqualTo("foo");

      var mapOfCache = cache.asMap();
      assertThat(mapOfCache).containsKey("foo");
    }
  }

  @Test
  public void entryCountAndHeapBytes_emptyBeforeLoadAndPopulatedAfter() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedTwoSegments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var cache = TokenFacetsStateCache.create(reader, Optional.empty());

      assertThat(cache.entryCount()).isEqualTo(0);
      assertThat(cache.heapBytes()).isEqualTo(0);

      assertThat(cache.get("foo")).isPresent();

      assertThat(cache.entryCount()).isEqualTo(1);
      assertThat(cache.heapBytes()).isGreaterThan(0);
    }
  }

  @Test
  public void snapshot_matchesIndividualReadsAfterLoad() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedTwoSegments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var cache = TokenFacetsStateCache.create(reader, Optional.empty());
      cache.get("foo");

      TokenFacetsStateCache.Snapshot snapshot = cache.snapshot();
      assertThat(snapshot.entries()).isEqualTo(cache.entryCount());
      assertThat(snapshot.heapBytes()).isEqualTo(cache.heapBytes());
      assertThat(snapshot.hits()).isEqualTo(cache.hitCount());
      assertThat(snapshot.misses()).isEqualTo(cache.missCount());
      assertThat(snapshot.evictions()).isEqualTo(cache.evictionCount());
    }
  }

  @Test
  public void recordStatsOffByDefault_hitAndMissStayZero() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedTwoSegments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var cache = TokenFacetsStateCache.create(reader, Optional.empty());
      cache.get("foo");
      cache.get("foo");

      assertThat(cache.missCount()).isEqualTo(0);
      assertThat(cache.hitCount()).isEqualTo(0);
      assertThat(cache.snapshot().hits()).isEqualTo(0);
      assertThat(cache.snapshot().misses()).isEqualTo(0);
    }
  }

  @Test
  public void snapshotPlus_sumsFields() {
    var left = new TokenFacetsStateCache.Snapshot(1, 2, 3, 4, 5);
    var right = new TokenFacetsStateCache.Snapshot(10, 20, 30, 40, 50);
    assertThat(left.plus(right)).isEqualTo(new TokenFacetsStateCache.Snapshot(11, 22, 33, 44, 55));
  }

  /**
   * The cache is rebuilt on every reader refresh, so the gauges must read the live instance: the
   * clone carries the entry over and reports its own heap, not the old cache's.
   */
  @Test
  public void entryCountAndHeapBytes_survivesReaderRefresh() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedTwoSegments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var cache = TokenFacetsStateCache.create(reader, Optional.empty());
      cache.get("foo");

      var newCache = cache.cloneWithNewIndexReader(DirectoryReader.open(directory));

      assertThat(newCache.entryCount()).isEqualTo(1);
      assertThat(newCache.heapBytes()).isGreaterThan(0);
    }
  }

  @Test
  public void hitAndMissCounts_trackCacheOutcomes() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedTwoSegments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var cache = TokenFacetsStateCache.create(reader, Optional.empty(), true);

      assertThat(cache.hitCount()).isEqualTo(0);
      assertThat(cache.missCount()).isEqualTo(0);

      cache.get("foo");
      cache.get("foo");
      cache.get("foo");

      assertThat(cache.missCount()).isEqualTo(1);
      assertThat(cache.hitCount()).isEqualTo(2);
    }
  }

  /**
   * The reason these counts are carried forward rather than read straight off Caffeine: the cache
   * is rebuilt on every reader refresh, so per-instance counters would reset several times an hour
   * and read as a cache that never hits anything.
   */
  @Test
  public void counts_surviveAReaderRefresh() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedTwoSegments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var cache = TokenFacetsStateCache.create(reader, Optional.empty(), true);
      cache.get("foo");
      cache.get("foo");
      long hitsBefore = cache.hitCount();
      long missesBefore = cache.missCount();
      assertThat(hitsBefore).isEqualTo(1);
      assertThat(missesBefore).isEqualTo(1);

      var newCache = cache.cloneWithNewIndexReader(DirectoryReader.open(directory));

      // Inherited before the successor has served a single request.
      assertThat(newCache.hitCount()).isEqualTo(hitsBefore);
      assertThat(newCache.missCount()).isEqualTo(missesBefore);

      newCache.get("foo");
      assertThat(newCache.hitCount()).isEqualTo(hitsBefore + 1);
    }
  }

  @Test
  public void testCacheLoadsUpdateLastQueriedTime() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedDocuments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var ticker = new FakeTicker();
      var cache =
          TokenFacetsStateCache.create(
              reader, Duration.ofMillis(2), Optional.empty(), ticker::read);
      assertThat(cache.getTimeBeforeExpiring("foo")).isEmpty();

      cache.get("foo");
      var mapOfCache = cache.asMap();
      assertThat(mapOfCache).containsKey("foo");
      var entry = mapOfCache.get("foo");
      assertThat(entry).isNotNull();
      var timeBeforeExpire = cache.getTimeBeforeExpiring("foo");
      assertThat(timeBeforeExpire).isPresent();
      assertThat(timeBeforeExpire.get()).isEqualTo(Duration.ofMillis(2));

      ticker.advance(Duration.ofMillis(1));
      var advancedTimeBeforeExpire = cache.getTimeBeforeExpiring("foo");
      assertThat(advancedTimeBeforeExpire).isPresent();
      assertThat(advancedTimeBeforeExpire.get()).isEqualTo(Duration.ofMillis(1));

      cache.get("foo");
      var newMapOfCache = cache.asMap();
      assertThat(newMapOfCache).containsKey("foo");
      var newEntry = newMapOfCache.get("foo");
      assertThat(newEntry).isNotNull();
      var resetTimeBeforeExpire = cache.getTimeBeforeExpiring("foo");
      assertThat(resetTimeBeforeExpire).isPresent();
      assertThat(resetTimeBeforeExpire.get()).isEqualTo(Duration.ofMillis(2));
    }
  }

  @Test
  public void testCloneWithNewIndexReader() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedDocuments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var cache = TokenFacetsStateCache.create(reader, Optional.empty());
      cache.get("foo");

      var mapOfCache = cache.asMap();
      assertThat(mapOfCache).containsKey("foo");

      var newCache = cache.cloneWithNewIndexReader(DirectoryReader.open(directory));

      var mapOfNewCache = newCache.asMap();
      assertThat(mapOfNewCache.size()).isEqualTo(1);
      assertThat(mapOfNewCache).containsKey("foo");

      var entry = mapOfNewCache.get("foo");
      assertThat(entry).isPresent();
      assertThat(entry.get().getField()).isEqualTo("foo");
    }
  }

  @Test
  public void testCloneWithNewIndexReaderEvictsExpiredEntries() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedDocuments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var ticker = new FakeTicker();
      var cache =
          TokenFacetsStateCache.create(
              reader, Duration.ofMillis(2), Optional.empty(), ticker::read);
      assertThat(cache.get("foo")).isPresent();

      ticker.advance(Duration.ofMillis(1));
      assertThat(cache.asMap()).containsKey("foo");
      @Var var newCache = cache.cloneWithNewIndexReader(DirectoryReader.open(directory));
      assertThat(newCache.asMap()).containsKey("foo");

      ticker.advance(Duration.ofMillis(2));
      newCache = cache.cloneWithNewIndexReader(DirectoryReader.open(directory));
      assertThat(newCache.asMap().containsKey("foo")).isFalse();
    }
  }

  @Test
  public void testCloneWithNewIndexReaderEvictsEmptyEntries() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedUnrelatedDocument(writer);
      IndexReader reader = DirectoryReader.open(directory);
      var ticker = new FakeTicker();
      var cache =
          TokenFacetsStateCache.create(
              reader, Duration.ofMillis(1), Optional.empty(), ticker::read);
      // query non-existent field
      assertThat(cache.get("foo")).isEmpty();

      // should write empty entry into cache
      assertThat(cache.asMap()).containsKey("foo");
      assertThat(cache.asMap().get("foo")).isEmpty();

      // add documents and refresh reader
      seedDocuments(writer);
      var newCache = cache.cloneWithNewIndexReader(DirectoryReader.open(directory));

      // refreshed cache should have evicted empty entries
      assertThat(newCache.asMap()).doesNotContainKey("foo");

      // should write non-empty entry into cache
      assertThat(newCache.get("foo")).isPresent();
      assertThat(newCache.asMap()).containsKey("foo");
      assertThat(newCache.asMap().get("foo")).isPresent();
    }
  }

  @Test
  public void testEmptyFieldOutDoesNotBreakFieldPopulationLater() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedUnrelatedDocument(writer);
      seedDocuments(writer);
      IndexReader reader = DirectoryReader.open(directory);
      var ticker = new FakeTicker();
      @Var
      var cache =
          TokenFacetsStateCache.create(
              reader, Duration.ofMillis(1), Optional.empty(), ticker::read);

      assertThat(cache.get("foo")).isPresent();
      assertThat(cache.asMap()).containsKey("foo");
      assertThat(cache.asMap().get("foo")).isPresent();

      deleteDocuments(writer);
      cache = cache.cloneWithNewIndexReader(DirectoryReader.open(directory));
      // clone should have placed an empty entry
      assertThat(cache.asMap()).containsKey("foo");
      assertThat(cache.asMap().get("foo")).isEmpty();

      // add documents and refresh reader
      seedDocuments(writer);
      cache = cache.cloneWithNewIndexReader(DirectoryReader.open(directory));

      // refreshed cache should have evicted empty entries
      assertThat(cache.asMap()).doesNotContainKey("foo");

      // should write non-empty entry into cache
      assertThat(cache.get("foo")).isPresent();
      assertThat(cache.asMap()).containsKey("foo");
      assertThat(cache.asMap().get("foo")).isPresent();
    }
  }

  @Test
  public void testMaxCardinalityThrowsCardinalityException() throws IOException {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      seedDocuments(writer);

      IndexReader reader = DirectoryReader.open(directory);
      var cache = TokenFacetsStateCache.create(reader, Optional.of(1));
      Assert.assertThrows(
          TokenFacetsCardinalityLimitExceededException.class, () -> cache.get("foo"));
    }
  }

  /**
   * Seeds two segments. {@link TokenSsdvFacetState#ramBytesUsed} reports the OrdinalMap's heap, and
   * Lucene only builds an OrdinalMap across multiple segments -- so a single-segment index caches
   * an entry that legitimately holds no measurable heap.
   */
  private void seedTwoSegments(IndexWriter writer) throws IOException {
    seedDocuments(writer);
    Document doc = new Document();
    doc.add(new StringField("_id", "3", Field.Store.YES));
    doc.add(new SortedSetDocValuesField("foo", new BytesRef("facet3")));
    writer.addDocument(doc);
    writer.commit();
  }

  private void seedDocuments(IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(new StringField("_id", "1", Field.Store.YES));
    doc.add(new SortedSetDocValuesField("foo", new BytesRef("facet1")));
    Document doc2 = new Document();
    doc2.add(new StringField("_id", "2", Field.Store.YES));
    doc2.add(new SortedSetDocValuesField("foo", new BytesRef("facet2")));
    writer.addDocument(doc);
    writer.addDocument(doc2);
    writer.commit();
  }

  private void deleteDocuments(IndexWriter writer) throws IOException {
    writer.deleteDocuments(new Term("_id", new BytesRef("1")));
    writer.deleteDocuments(new Term("_id", new BytesRef("2")));
    writer.forceMergeDeletes();
    writer.commit();
  }

  private void seedUnrelatedDocument(IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("bar", new BytesRef("facet1")));
    writer.addDocument(doc);
    writer.commit();
  }
}
