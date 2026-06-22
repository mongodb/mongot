package com.xgen.testing.fakes;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.util.BytesRef;

/**
 * A fake StoredFields implementation that can be instantiated by a HashMap rather than writing an
 * index. This is needed because StoredFields has final methods that cannot be mocked.
 */
public class FakeStoredFields extends StoredFields {

  private final IntFunction<Document> docs;

  /**
   * Create a StoredFields implementation that computes stored fields for a given docId on the fly
   */
  public FakeStoredFields(IntFunction<Document> docs) {
    this.docs = docs;
  }

  /**
   * Create a StoredFields implementation that returns a precomputed set of fields from the map.
   * docIds not present in the map are implicitly mapped to the empty document.
   */
  public FakeStoredFields(Map<Integer, Document> docs) {
    this(doc -> docs.getOrDefault(doc, new Document()));
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    Document doc = this.docs.apply(docID);

    for (IndexableField field : doc) {
      FieldInfo info = createFieldInfo(field);
      switch (visitor.needsField(info)) {
        case YES -> {}
        case NO -> {
          continue;
        }
        case STOP -> {
          return;
        }
      }

      var stored = field.storedValue();
      switch (stored.getType()) {
        case INTEGER -> visitor.intField(info, stored.getIntValue());
        case LONG -> visitor.longField(info, stored.getLongValue());
        case FLOAT -> visitor.floatField(info, stored.getFloatValue());
        case DOUBLE -> visitor.doubleField(info, stored.getDoubleValue());
        case BINARY -> visitor.binaryField(info, toArray(stored.getBinaryValue()));
        case STRING -> visitor.stringField(info, stored.getStringValue());
      }
    }
  }

  private static FieldInfo createFieldInfo(IndexableField field) {
    IndexableFieldType type = field.fieldType();
    return new FieldInfo(
        field.name(),
        1,
        type.storeTermVectors(),
        type.omitNorms(),
        type.storeTermVectorPayloads(),
        type.indexOptions(),
        type.docValuesType(),
        DocValuesSkipIndexType.NONE,
        type.docValuesType() == DocValuesType.NONE ? -1 : 1,
        new HashMap<>(),
        type.pointDimensionCount(),
        type.pointIndexDimensionCount(),
        type.pointNumBytes(),
        type.vectorDimension(),
        type.vectorEncoding(),
        type.vectorSimilarityFunction(),
        false,
        false);
  }

  private static byte[] toArray(BytesRef ref) {
    return Arrays.copyOfRange(ref.bytes, ref.offset, ref.length);
  }
}
