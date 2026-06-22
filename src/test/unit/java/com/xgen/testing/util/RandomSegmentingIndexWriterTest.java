package com.xgen.testing.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;

public class RandomSegmentingIndexWriterTest {

  private static Document create() {
    Document d = new Document();
    d.add(new NumericDocValuesField("Field", 1));
    return d;
  }

  @SuppressWarnings({"deprecation"}) // Testing deprecated addDocument(doc) method
  private static int getStreamedSegmentCount(int numDocs, Random rng) throws IOException {
    try (var directory = new ByteBuffersDirectory()) {
      try (var writer = new RandomSegmentingIndexWriter(directory, new IndexWriterConfig(), rng)) {
        List<Document> docs = Collections.nCopies(numDocs, create());

        for (Document doc : docs) {
          writer.addDocument(doc);
        }
      }
      return DirectoryReader.open(directory).leaves().size();
    }
  }

  private static int getBulkAddSegmentCount(int numDocs, Random rng) throws IOException {
    try (var directory = new ByteBuffersDirectory()) {
      try (var writer = new RandomSegmentingIndexWriter(directory, new IndexWriterConfig(), rng)) {
        List<Document> docs = Collections.nCopies(numDocs, create());
        writer.addDocuments(docs);
      }

      return DirectoryReader.open(directory).leaves().size();
    }
  }

  @Test
  public void addSingleDocumentCreatesMultipleSegments() throws IOException {
    assertEquals(0, getStreamedSegmentCount(0, new Random(42)));
    assertEquals(1, getStreamedSegmentCount(1, new Random(42)));
    assertEquals(3, getStreamedSegmentCount(10, new Random(42)));
    assertEquals(4, getStreamedSegmentCount(100, new Random(42)));
  }

  @Test
  public void bulkAddCreatesMultipleSegments() throws IOException {
    assertEquals(0, getBulkAddSegmentCount(0, new Random(42)));
    assertEquals(1, getBulkAddSegmentCount(1, new Random(42)));
    assertEquals(5, getBulkAddSegmentCount(10, new Random(42)));
    assertEquals(10, getBulkAddSegmentCount(100, new Random(42)));
  }

  @Test
  public void nonDeterministicWriterNeverThrows() throws IOException {
    assertThat(getStreamedSegmentCount(10, new Random())).isAtMost(10);
    assertThat(getBulkAddSegmentCount(10, new Random())).isAtMost(10);
  }
}
