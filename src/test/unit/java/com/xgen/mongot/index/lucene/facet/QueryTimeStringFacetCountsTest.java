package com.xgen.mongot.index.lucene.facet;

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/** Unit coverage for query-time token facet counting. */
public class QueryTimeStringFacetCountsTest {

  private static final String GROUP_FIELD = "group";
  private static final String TOKEN_FIELD = "foo";

  @Test
  public void forTokenField_exactHits_returnsTopChildrenInCountOrder() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addTokenDoc(writer, "hit", "aaa");
      addTokenDoc(writer, "hit", "aaa");
      addTokenDoc(writer, "hit", "aaa");
      addTokenDoc(writer, "hit", "bbb");
      addTokenDoc(writer, "hit", "bbb");
      addTokenDoc(writer, "hit", "ccc");
      addTokenDoc(writer, "miss", "zzz");
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        FacetsCollector collector = collect(reader, "hit");
        var counts = QueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector);

        FacetResult top = counts.getTopChildren(2, TOKEN_FIELD);
        assertThat(labelValues(top))
            .containsExactly(new SimpleLabel("aaa", 3), new SimpleLabel("bbb", 2))
            .inOrder();
        assertThat(top.value.longValue()).isEqualTo(6);
        assertThat(top.childCount).isEqualTo(3);
      }
    }
  }

  @Test
  public void forTokenField_sameHits_matchesReaderStateCounts() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addTokenDoc(writer, "hit", "d");
      addTokenDoc(writer, "hit", "b");
      addTokenDoc(writer, "hit", "a");
      addTokenDoc(writer, "hit", "c");
      addTokenDoc(writer, "hit", "b");
      addTokenDoc(writer, "hit", "a");
      addTokenDoc(writer, "hit", "a");
      addTokenDoc(writer, "hit", "e");
      addTokenDoc(writer, "miss", "z");
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        FacetsCollector collector = collect(reader, "hit");

        var queryTime = QueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector);
        SortedSetDocValuesReaderState state =
            TokenSsdvFacetState.create(reader, TOKEN_FIELD, Optional.empty()).orElseThrow();
        var readerStateCounts = new SortedSetDocValuesFacetCounts(state, collector);

        assertSameFacetResult(
            readerStateCounts.getTopChildren(3, TOKEN_FIELD),
            queryTime.getTopChildren(3, TOKEN_FIELD));
        assertSameFacetResult(
            readerStateCounts.getTopChildren(100, TOKEN_FIELD),
            queryTime.getTopChildren(100, TOKEN_FIELD));
      }
    }
  }

  @Test
  public void forTokenField_equalCounts_breaksTiesInLuceneOrdinalOrder() throws Exception {
    // U+E000 sorts before U+10000 as unsigned UTF-8, and after it in Java UTF-16 order.
    String earlierInOrdinalOrder = new String(Character.toChars(0xE000));
    String laterInOrdinalOrder = new String(Character.toChars(0x10000));
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addTokenDoc(writer, "hit", earlierInOrdinalOrder);
      addTokenDoc(writer, "hit", laterInOrdinalOrder);
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        FacetsCollector collector = collect(reader, "hit");
        var queryTime = QueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector);
        SortedSetDocValuesReaderState state =
            TokenSsdvFacetState.create(reader, TOKEN_FIELD, Optional.empty()).orElseThrow();
        var readerStateCounts = new SortedSetDocValuesFacetCounts(state, collector);

        assertThat(queryTime.getTopChildren(1, TOKEN_FIELD).labelValues[0].label)
            .isEqualTo(earlierInOrdinalOrder);
        assertSameFacetResult(
            readerStateCounts.getTopChildren(1, TOKEN_FIELD),
            queryTime.getTopChildren(1, TOKEN_FIELD));
        assertSameFacetResult(
            readerStateCounts.getAllChildren(TOKEN_FIELD), queryTime.getAllChildren(TOKEN_FIELD));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
          var concurrent =
              ConcurrentQueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector, executor);
          assertSameFacetResult(
              queryTime.getTopChildren(1, TOKEN_FIELD), concurrent.getTopChildren(1, TOKEN_FIELD));
          assertSameFacetResult(
              queryTime.getAllChildren(TOKEN_FIELD), concurrent.getAllChildren(TOKEN_FIELD));
        } finally {
          executor.shutdownNow();
        }
      }
    }
  }

  @Test
  public void forTokenField_noMatchingDocs_returnsNoBuckets() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addTokenDoc(writer, "hit", "aaa");
      addTokenDoc(writer, "hit", "bbb");
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        FacetsCollector collector = collect(reader, "no-such-group");
        var counts = QueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector);

        assertThat(counts.getTopChildren(5, TOKEN_FIELD).labelValues).isEmpty();
        assertThat(counts.getAllChildren(TOKEN_FIELD).labelValues).isEmpty();
      }
    }
  }

  @Test
  public void forTokenField_missingField_returnsEmpty() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addTokenDoc(writer, "hit", "aaa");
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        FacetsCollector collector = collect(reader, "hit");
        var counts = QueryTimeStringFacetCounts.forTokenField("never-indexed", collector);

        assertThat(counts.getAllChildren("never-indexed").labelValues).isEmpty();
        assertThat(counts.getTopChildren(5, "never-indexed").value.longValue()).isEqualTo(0);
      }
    }
  }

  @Test
  public void forTokenField_multiSegment_mergesTermsAcrossLocalOrdinals() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(
                directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      // Segment 1 ordinals: m=0, z=1. Segment 2 ordinals: a=0, m=1.
      addTokenDoc(writer, "hit", "m");
      addTokenDoc(writer, "hit", "z");
      writer.commit();
      addTokenDoc(writer, "hit", "a");
      addTokenDoc(writer, "hit", "m");
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        assertThat(reader.leaves().size()).isAtLeast(2);
        FacetsCollector collector = collect(reader, "hit");
        var counts = QueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector);

        assertThat(labelValues(counts.getAllChildren(TOKEN_FIELD)))
            .containsExactly(
                new SimpleLabel("a", 1), new SimpleLabel("m", 2), new SimpleLabel("z", 1));
      }
    }
  }

  @Test
  public void forTokenField_multiValuedDoc_countsEachValue() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField(GROUP_FIELD, "hit", Field.Store.NO));
      doc.add(new SortedSetDocValuesField(TOKEN_FIELD, new BytesRef("aaa")));
      doc.add(new SortedSetDocValuesField(TOKEN_FIELD, new BytesRef("bbb")));
      writer.addDocument(doc);
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        FacetsCollector collector = collect(reader, "hit");
        var counts = QueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector);

        assertThat(labelValues(counts.getAllChildren(TOKEN_FIELD)))
            .containsExactly(new SimpleLabel("aaa", 1), new SimpleLabel("bbb", 1));
      }
    }
  }

  @Test
  public void forTokenField_counting_doesNotPopulateTokenFacetsStateCache() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addTokenDoc(writer, "hit", "aaa");
      addTokenDoc(writer, "hit", "aaa");
      addTokenDoc(writer, "hit", "bbb");
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        var cache = TokenFacetsStateCache.create(reader, Optional.empty());
        assertThat(cache.asMap()).isEmpty();

        FacetsCollector collector = collect(reader, "hit");
        var counts = QueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector);
        FacetResult top = counts.getTopChildren(10, TOKEN_FIELD);

        assertThat(labelValues(top))
            .containsExactly(new SimpleLabel("aaa", 2), new SimpleLabel("bbb", 1))
            .inOrder();
        assertThat(cache.asMap()).isEmpty();
      }
    }
  }

  @Test
  public void forTokenField_multiSegment_concurrentCountsMatchSequential() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(
                directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      addTokenDoc(writer, "hit", "m");
      addTokenDoc(writer, "hit", "z");
      writer.commit();
      addTokenDoc(writer, "hit", "a");
      addTokenDoc(writer, "hit", "m");
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        FacetsCollector collector = collect(reader, "hit");
        var sequential = QueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
          var concurrent =
              ConcurrentQueryTimeStringFacetCounts.forTokenField(TOKEN_FIELD, collector, executor);
          assertSameFacetResult(
              sequential.getAllChildren(TOKEN_FIELD), concurrent.getAllChildren(TOKEN_FIELD));
        } finally {
          executor.shutdownNow();
        }
      }
    }
  }

  private record SimpleLabel(String label, long value) {}

  private static List<SimpleLabel> labelValues(FacetResult result) {
    List<SimpleLabel> out = new ArrayList<>();
    for (LabelAndValue labelAndValue : result.labelValues) {
      out.add(new SimpleLabel(labelAndValue.label, labelAndValue.value.longValue()));
    }
    return out;
  }

  private static void assertSameFacetResult(FacetResult expected, FacetResult actual) {
    assertThat(labelValues(actual)).isEqualTo(labelValues(expected));
    assertThat(actual.value.longValue()).isEqualTo(expected.value.longValue());
    assertThat(actual.childCount).isEqualTo(expected.childCount);
  }

  private static FacetsCollector collect(IndexReader reader, String group) throws Exception {
    IndexSearcher searcher = new IndexSearcher(reader);
    var result =
        FacetsCollectorManager.search(
            searcher, new TermQuery(new Term(GROUP_FIELD, group)), 1, new FacetsCollectorManager());
    return result.facetsCollector();
  }

  private static void addTokenDoc(IndexWriter writer, String group, String value) throws Exception {
    Document doc = new Document();
    doc.add(new StringField(GROUP_FIELD, group, Field.Store.NO));
    doc.add(new SortedSetDocValuesField(TOKEN_FIELD, new BytesRef(value)));
    writer.addDocument(doc);
  }
}
