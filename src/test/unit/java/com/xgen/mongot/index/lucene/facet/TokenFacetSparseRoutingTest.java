package com.xgen.mongot.index.lucene.facet;

import static com.google.common.truth.Truth.assertThat;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

public class TokenFacetSparseRoutingTest {

  private static final String FIELD = "facet";

  @Test
  public void isHitRatioSparse_emptyIndex_isSparse() {
    assertThat(TokenFacetSparseRouting.isHitRatioSparse(0, 0)).isTrue();
  }

  @Test
  public void isHitRatioSparse_exactlyTenPercent_isSparse() {
    assertThat(TokenFacetSparseRouting.isHitRatioSparse(1, 10)).isTrue();
  }

  @Test
  public void isHitRatioSparse_aboveTenPercent_isDense() {
    assertThat(TokenFacetSparseRouting.isHitRatioSparse(2, 10)).isFalse();
  }

  @Test
  public void isSparse_cardinalityBelowLimit_staysDense() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      // Twenty docs and two values: the hit ratio is sparse, the ordinal map is still cheap.
      addCopies(writer, "a", 10);
      addCopies(writer, "b", 10);
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        boolean sparse = TokenFacetSparseRouting.isSparse(1, reader.maxDoc(), reader, FIELD);

        assertThat(sparse).isFalse();
      }
    }
  }

  @Test
  public void isSparse_cardinalityAtLimitAndSelectiveHits_isSparse() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addDistinctTerms(writer, "v", SortedSetDocValuesFacetCounts.DENSE_CARDINALITY_LIMIT);
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        boolean sparse = TokenFacetSparseRouting.isSparse(1, reader.maxDoc(), reader, FIELD);

        assertThat(sparse).isTrue();
      }
    }
  }

  @Test
  public void isSparse_oneUnderLimit_staysDense() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addDistinctTerms(writer, "v", SortedSetDocValuesFacetCounts.DENSE_CARDINALITY_LIMIT - 1);
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        boolean sparse = TokenFacetSparseRouting.isSparse(1, reader.maxDoc(), reader, FIELD);

        assertThat(sparse).isFalse();
      }
    }
  }

  @Test
  public void isSparse_splitAcrossSegments_usesSumOfSegmentCardinalities() throws Exception {
    int half = SortedSetDocValuesFacetCounts.DENSE_CARDINALITY_LIMIT / 2;
    try (var directory = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(
                directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      addDistinctTerms(writer, "a", half);
      writer.commit();
      addDistinctTerms(writer, "b", half);
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        assertThat(reader.leaves()).hasSize(2);

        boolean sparse = TokenFacetSparseRouting.isSparse(1, reader.maxDoc(), reader, FIELD);

        assertThat(sparse).isTrue();
      }
    }
  }

  @Test
  public void isSparse_highCardinalityButBroadHits_isDense() throws Exception {
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addDistinctTerms(writer, "v", SortedSetDocValuesFacetCounts.DENSE_CARDINALITY_LIMIT);
      writer.commit();

      try (IndexReader reader = DirectoryReader.open(directory)) {
        boolean sparse =
            TokenFacetSparseRouting.isSparse(reader.maxDoc(), reader.maxDoc(), reader, FIELD);

        assertThat(sparse).isFalse();
      }
    }
  }

  private static void addDistinctTerms(IndexWriter writer, String prefix, int count)
      throws Exception {
    for (int i = 0; i < count; i++) {
      addCopies(writer, prefix + i, 1);
    }
  }

  private static void addCopies(IndexWriter writer, String value, int count) throws Exception {
    BytesRef term = new BytesRef(value);
    for (int i = 0; i < count; i++) {
      Document doc = new Document();
      doc.add(new SortedSetDocValuesField(FIELD, term));
      writer.addDocument(doc);
    }
  }
}
