package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.ByteUtils;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Test;

/**
 * Tests {@link FieldValueFetcher}, which for private preview reads values only from stored source:
 * scalars, arrays, and nested paths come back with full BSON fidelity; an absent path is empty.
 */
public class FieldValueFetcherTest {

  private static final String STORED_SOURCE_FIELD =
      FieldName.StaticField.STORED_SOURCE.getLuceneFieldName();

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

  @Test
  public void scalarValue_readsFromSource() throws IOException {
    openIndex(new BsonDocument("price", new BsonInt32(7)));
    assertEquals(Optional.of(new BsonInt32(7)), fetch(FieldPath.parse("price")));
  }

  @Test
  public void arrayValue_readsWholeArray() throws IOException {
    BsonArray array = new BsonArray(List.of(new BsonInt32(3), new BsonInt32(9)));
    openIndex(new BsonDocument("price", array));
    assertEquals(Optional.of(array), fetch(FieldPath.parse("price")));
  }

  @Test
  public void nestedPath_walksDocuments() throws IOException {
    openIndex(new BsonDocument("a", new BsonDocument("b", new BsonString("x"))));
    assertEquals(Optional.of(new BsonString("x")), fetch(FieldPath.parse("a.b")));
  }

  @Test
  public void absentPath_returnsEmpty() throws IOException {
    openIndex(new BsonDocument("other", new BsonInt32(1)));
    assertTrue(fetch(FieldPath.parse("price")).isEmpty());
  }

  @Test
  public void documentWithoutStoredSource_returnsEmpty() throws IOException {
    openIndex(new Document());
    assertTrue(fetch(FieldPath.parse("price")).isEmpty());
  }

  // ===== helpers =====

  private Optional<BsonValue> fetch(FieldPath path) throws IOException {
    LeafReaderContext leaf = this.reader.leaves().get(0);
    return FieldValueFetcher.create(new ResolvedTokenField.FromStoredSource(path), leaf).fetch(0);
  }

  private void openIndex(BsonDocument source) throws IOException {
    Document luceneDoc = new Document();
    luceneDoc.add(new StoredField(STORED_SOURCE_FIELD, ByteUtils.toByteArray(source)));
    openIndex(luceneDoc);
  }

  private void openIndex(Document luceneDoc) throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      writer.addDocument(luceneDoc);
    }
    this.reader = DirectoryReader.open(this.directory);
  }
}
