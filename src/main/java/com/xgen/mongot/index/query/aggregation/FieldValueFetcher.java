package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.ByteUtils;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Reads a token-valued metric's value from a path in one segment, for operators such as {@code
 * first}/{@code last} and {@code min}/{@code max}.
 *
 * <p>Built once per segment. The underlying stored-fields reader is optimized for sequential
 * access, so callers should request documents in increasing segment-local doc-id order (Lucene
 * collection and bucket-winner reads both satisfy this).
 *
 * <p>Values come from whichever source the path resolved to: stored source, for full BSON
 * type/shape fidelity at the cost of a per-document stored-fields fetch, or the path's per-type
 * doc-values columns. {@code LuceneAggregationContextFieldResolver#resolveTokenMetricField} picks
 * between them once per query.
 */
final class FieldValueFetcher {

  private final ValueSource source;

  private FieldValueFetcher(ValueSource source) {
    this.source = source;
  }

  static FieldValueFetcher create(ResolvedTokenField field, LeafReaderContext leaf)
      throws IOException {
    return new FieldValueFetcher(
        switch (field) {
          case ResolvedTokenField.FromStoredSource storedSource ->
              new StoredSourceReader(storedSource.path(), leaf);
          case ResolvedTokenField.FromDocValues docValues ->
              DocValuesFieldReader.create(docValues.path(), docValues.types(), leaf);
        });
  }

  /** The value for {@code leafDoc}, or empty when the source holds none for it. */
  Optional<BsonValue> fetch(int leafDoc) throws IOException {
    return this.source.read(leafDoc);
  }

  /** One segment's supply of a token-valued metric's values. */
  interface ValueSource {
    /** The value for {@code leafDoc}, or empty when this source holds none for it. */
    Optional<BsonValue> read(int leafDoc) throws IOException;
  }

  /** Reads the document's stored source and walks it to the path. */
  private static final class StoredSourceReader implements ValueSource {
    private static final String STORED_SOURCE_FIELD =
        FieldName.StaticField.STORED_SOURCE.getLuceneFieldName();
    private static final Set<String> STORED_FIELDS = Set.of(STORED_SOURCE_FIELD);

    private final FieldPath path;
    private final StoredFields storedFields;

    StoredSourceReader(FieldPath path, LeafReaderContext leaf) throws IOException {
      this.path = path;
      this.storedFields = leaf.reader().storedFields();
    }

    @Override
    public Optional<BsonValue> read(int leafDoc) throws IOException {
      // getBinaryValue returns null for a document with no stored source.
      return Optional.ofNullable(
              this.storedFields
                  .document(leafDoc, STORED_FIELDS)
                  .getBinaryValue(STORED_SOURCE_FIELD))
          .flatMap(bytes -> navigate(ByteUtils.fromBytesRef(bytes), this.path));
    }

    /**
     * Walks {@code path} through nested documents and returns the value at the final segment, which
     * may be any BSON type (scalar, array, document). The isDocument filter applies only to the
     * containers being descended through: an intermediate segment that is not a document (e.g. an
     * array on the way to {@code a.b.c}) returns empty rather than fanning out.
     */
    private static Optional<BsonValue> navigate(BsonDocument source, FieldPath path) {
      @Var Optional<BsonValue> current = Optional.of(source);
      for (String segment : path.getSegments()) {
        current =
            current
                .filter(BsonValue::isDocument)
                .map(BsonValue::asDocument)
                // get returns null for an absent key.
                .flatMap(document -> Optional.ofNullable(document.get(segment)));
      }
      return current;
    }
  }
}
