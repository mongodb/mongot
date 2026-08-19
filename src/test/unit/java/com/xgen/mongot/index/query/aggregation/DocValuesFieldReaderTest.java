package com.xgen.mongot.index.query.aggregation;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.sort.mixed.SortUtil;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.aggregation.DocValuesFieldReader.Selection;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Test;

/**
 * Tests {@link DocValuesFieldReader}.
 *
 * <p>Documents are written with their doc-values fields directly, using the same production helpers
 * {@code IndexableFieldFactory} uses, so these cases stop at the column: they assert what the
 * reader returns for a given term or encoded long, not what the indexer would have put there.
 * Normalization, truncation and representation narrowing belong to {@code IndexableFieldFactory}
 * and are covered by the aggregation e2e specs.
 */
public class DocValuesFieldReaderTest {

  private static final FieldPath PATH = FieldPath.parse("a.b");

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
  public void read_singleValue_returnsScalar() throws IOException {
    openIndex(document(tokenField("boston")));

    assertThat(read(Set.of(TypeField.TOKEN), 0)).hasValue(new BsonString("boston"));
  }

  @Test
  public void read_severalValuesOfOneType_returnsArrayInValueOrder() throws IOException {
    openIndex(document(int64Field(9), int64Field(3)));

    assertThat(read(Set.of(TypeField.NUMBER_INT64_V2), 0))
        .hasValue(new BsonArray(List.of(new BsonInt64(3), new BsonInt64(9))));
  }

  @Test
  public void read_repeatedNumericValue_keepsEveryOccurrence() throws IOException {
    openIndex(document(int64Field(3), int64Field(1), int64Field(3)));

    assertThat(read(Set.of(TypeField.NUMBER_INT64_V2), 0))
        .hasValue(new BsonArray(List.of(new BsonInt64(1), new BsonInt64(3), new BsonInt64(3))));
  }

  @Test
  public void read_repeatedTokenValue_isDeduplicatedByTheColumn() throws IOException {
    openIndex(document(tokenField("boston"), tokenField("boston")));

    assertThat(read(Set.of(TypeField.TOKEN), 0)).hasValue(new BsonString("boston"));
  }

  @Test
  public void read_severalTypesAtOnePath_ordersValuesByBsonTypeBracket() throws IOException {
    openIndex(document(booleanField(true), tokenField("boston"), int64Field(7)));

    assertThat(read(Set.of(TypeField.BOOLEAN, TypeField.TOKEN, TypeField.NUMBER_INT64_V2), 0))
        .hasValue(
            new BsonArray(List.of(new BsonInt64(7), new BsonString("boston"), BsonBoolean.TRUE)));
  }

  @Test
  public void read_documentWithoutAValueAtThePath_returnsEmpty() throws IOException {
    openIndex(document(tokenField("boston")), new Document());

    assertThat(read(Set.of(TypeField.TOKEN), 1)).isEmpty();
  }

  /**
   * The production shape for a multi-type path: the definition allows several types but no document
   * in this segment carried one of them, so that column does not exist and is skipped.
   */
  @Test
  public void read_someTypesWithNoColumnInTheSegment_readsTheOnesPresent() throws IOException {
    openIndex(document(tokenField("boston"), int64Field(7)));

    assertThat(read(Set.of(TypeField.TOKEN, TypeField.NUMBER_INT64_V2, TypeField.DATE_V2), 0))
        .hasValue(new BsonArray(List.of(new BsonInt64(7), new BsonString("boston"))));
  }

  @Test
  public void read_laterDocumentInTheSegment_advancesToIt() throws IOException {
    openIndex(document(tokenField("boston")), document(tokenField("dublin")));

    assertThat(read(Set.of(TypeField.TOKEN), 1)).hasValue(new BsonString("dublin"));
  }

  @Test
  public void read_doubleValue_decodesFromTheSortableLong() throws IOException {
    openIndex(document(doubleField(-1.5)));

    assertThat(read(Set.of(TypeField.NUMBER_DOUBLE_V2), 0)).hasValue(new BsonDouble(-1.5));
  }

  @Test
  public void read_dateValue_decodesToDateTime() throws IOException {
    openIndex(document(dateField(1_700_000_000_000L)));

    assertThat(read(Set.of(TypeField.DATE_V2), 0)).hasValue(new BsonDateTime(1_700_000_000_000L));
  }

  @Test
  public void read_falseBooleanValue_decodesToFalse() throws IOException {
    openIndex(document(booleanField(false)));

    assertThat(read(Set.of(TypeField.BOOLEAN), 0)).hasValue(BsonBoolean.FALSE);
  }

  @Test
  public void read_nullValue_decodesToBsonNull() throws IOException {
    openIndex(document(nullField()));

    assertThat(read(Set.of(TypeField.NULL), 0)).hasValue(BsonNull.VALUE);
  }

  @Test
  public void read_objectIdValue_decodesToObjectId() throws IOException {
    ObjectId objectId = new ObjectId("507f1f77bcf86cd799439011");
    openIndex(document(objectIdField(objectId)));

    assertThat(read(Set.of(TypeField.OBJECT_ID), 0)).hasValue(new BsonObjectId(objectId));
  }

  @Test
  public void read_uuidValue_decodesToBinary() throws IOException {
    UUID uuid = UUID.fromString("b7c1a3d2-0000-4000-8000-000000000001");
    openIndex(document(uuidField(uuid)));

    assertThat(read(Set.of(TypeField.UUID), 0)).hasValue(new BsonBinary(uuid));
  }

  /** What {@code min} does: take the lowest value and decode nothing else. */
  @Test
  public void columns_first_yieldsTheColumnsLowestValue() throws IOException {
    openIndex(document(int64Field(9), int64Field(3), int64Field(7)));

    assertThat(columns(Set.of(TypeField.NUMBER_INT64_V2), Selection.MIN).get(0).values(0))
        .containsExactly(new BsonInt64(3));
  }

  /** What {@code max} does: skip to the highest value, still decoding one. */
  @Test
  public void columns_last_yieldsTheColumnsHighestValue() throws IOException {
    openIndex(document(int64Field(9), int64Field(3), int64Field(7)));

    assertThat(columns(Set.of(TypeField.NUMBER_INT64_V2), Selection.MAX).get(0).values(0))
        .containsExactly(new BsonInt64(9));
  }

  /** last() decodes an ordinal after skipping, so the term still round-trips. */
  @Test
  public void columns_lastOnATokenColumn_decodesTheHighestTerm() throws IOException {
    openIndex(document(tokenField("boston"), tokenField("dublin"), tokenField("austin")));

    assertThat(columns(Set.of(TypeField.TOKEN), Selection.MAX).get(0).values(0))
        .containsExactly(new BsonString("dublin"));
  }

  /**
   * The reader declares its column order rather than computing it, so this checks the declaration
   * against the real bracket table: reading the columns in order has to walk BSON brackets upwards,
   * or a document's values come out unordered.
   */
  @Test
  public void columns_declaredOrder_matchesBsonBracketPriority() throws IOException {
    openIndex(
        document(
            nullField(),
            doubleField(1.5),
            tokenField("boston"),
            uuidField(UUID.fromString("b7c1a3d2-0000-4000-8000-000000000001")),
            objectIdField(new ObjectId("507f1f77bcf86cd799439011")),
            booleanField(true),
            dateField(1_700_000_000_000L)));

    List<BsonValue> values =
        columns(
                Set.of(
                    TypeField.NULL,
                    TypeField.NUMBER_DOUBLE_V2,
                    TypeField.TOKEN,
                    TypeField.UUID,
                    TypeField.OBJECT_ID,
                    TypeField.BOOLEAN,
                    TypeField.DATE_V2),
                Selection.MIN)
            .stream()
            .map(column -> lowest(column, 0))
            .toList();

    List<Integer> brackets =
        values.stream()
            .map(v -> SortUtil.getBracketPriority(v.getBsonType(), NullEmptySortPosition.LOWEST))
            .toList();

    assertThat(brackets).isInStrictOrder();
  }

  @Test
  public void create_typeWithNoDocValuesColumn_throws() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              openIndex(document(tokenField("boston")));
              columns(Set.of(TypeField.TOKEN, TypeField.AUTOCOMPLETE), Selection.ALL);
            });

    assertThat(thrown).hasMessageThat().contains("AUTOCOMPLETE");
  }

  // ===== helpers =====

  /** {@link DocValuesFieldReader.Column#values} without the checked exception. */
  private static BsonValue lowest(DocValuesFieldReader.Column column, int leafDoc) {
    try {
      return column.values(leafDoc).get(0);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private List<DocValuesFieldReader.Column> columns(Set<TypeField> types, Selection selection)
      throws IOException {
    LeafReaderContext leaf = this.reader.leaves().get(0);
    return DocValuesFieldReader.create(PATH, types, leaf, selection).columns();
  }

  private Optional<BsonValue> read(Set<TypeField> types, int leafDoc) throws IOException {
    LeafReaderContext leaf = this.reader.leaves().get(0);
    return DocValuesFieldReader.create(PATH, types, leaf).read(leafDoc);
  }

  private static String luceneField(TypeField type) {
    return type.getLuceneFieldName(PATH, Optional.empty());
  }

  private static IndexableField int64Field(long value) {
    return new SortedNumericDocValuesField(luceneField(TypeField.NUMBER_INT64_V2), value);
  }

  private static IndexableField doubleField(double value) {
    return new SortedNumericDocValuesField(
        luceneField(TypeField.NUMBER_DOUBLE_V2),
        LuceneDoubleConversionUtils.toMqlSortableLong(value));
  }

  private static IndexableField dateField(long millis) {
    return new SortedNumericDocValuesField(luceneField(TypeField.DATE_V2), millis);
  }

  private static IndexableField tokenField(String value) {
    return new SortedSetDocValuesField(luceneField(TypeField.TOKEN), new BytesRef(value));
  }

  private static IndexableField booleanField(boolean value) {
    return new SortedSetDocValuesField(
        luceneField(TypeField.BOOLEAN), new BytesRef(FieldValue.fromBoolean(value)));
  }

  private static IndexableField objectIdField(ObjectId value) {
    return new SortedSetDocValuesField(
        luceneField(TypeField.OBJECT_ID), new BytesRef(value.toByteArray()));
  }

  private static IndexableField uuidField(UUID value) {
    return new SortedSetDocValuesField(luceneField(TypeField.UUID), new BytesRef(value.toString()));
  }

  private static IndexableField nullField() {
    return new SortedDocValuesField(luceneField(TypeField.NULL), new BytesRef());
  }

  private static Document document(IndexableField... fields) {
    Document luceneDoc = new Document();
    for (IndexableField field : fields) {
      luceneDoc.add(field);
    }
    return luceneDoc;
  }

  /** Writes all documents in one session so they share a single segment. */
  private void openIndex(Document... documents) throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (Document luceneDoc : documents) {
        writer.addDocument(luceneDoc);
      }
    }
    this.reader = DirectoryReader.open(this.directory);
  }
}
