package com.xgen.mongot.index.lucene;

import static org.junit.Assert.assertThrows;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Lucene forbids a field changing its {@link IndexOptions} within a single index. This is the
 * constraint that forces the autocomplete field-type change to be gated on the index feature
 * version (a rebuild) rather than flipped in place. A commit / segment boundary does not help: the
 * consistency check is index-global (persisted in {@code FieldInfos}), not per-segment.
 */
@RunWith(JUnit4.class)
public class LuceneIndexOptionsConsistencyTest {

  private static IndexWriter newWriter(Directory dir) throws Exception {
    return new IndexWriter(dir, new IndexWriterConfig(new WhitespaceAnalyzer()));
  }

  private static Document doc(IndexOptions indexOptions) {
    FieldType fieldType = new FieldType();
    fieldType.setIndexOptions(indexOptions);
    fieldType.setTokenized(true);
    fieldType.freeze();
    Document document = new Document();
    document.add(new Field("name", "morton's steakhouse", fieldType));
    return document;
  }

  @Test
  public void addDocument_changedIndexOptionsInSameWriter_throws() throws Exception {
    try (Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = newWriter(dir)) {
      writer.addDocument(doc(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS));
      assertThrows(
          IllegalArgumentException.class,
          () -> writer.addDocument(doc(IndexOptions.DOCS_AND_FREQS)));
    }
  }

  @Test
  public void addDocument_changedIndexOptionsAfterCommit_throws() throws Exception {
    try (Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = newWriter(dir)) {
      writer.addDocument(doc(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS));
      writer.commit();
      assertThrows(
          IllegalArgumentException.class,
          () -> writer.addDocument(doc(IndexOptions.DOCS_AND_FREQS)));
    }
  }
}
