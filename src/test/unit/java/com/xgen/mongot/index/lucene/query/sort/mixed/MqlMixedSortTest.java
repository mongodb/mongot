package com.xgen.mongot.index.lucene.query.sort.mixed;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.util.RandomSegmentingIndexWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MqlMixedSortTest {

  private Directory directory;
  private IndexSearcher searcher;

  private static final String stringField =
      FieldName.TypeField.TOKEN.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());

  private static final String dateField =
      FieldName.TypeField.DATE_V2.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());
  private static final String intField =
      FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
          FieldPath.newRoot("f"), Optional.empty());
  private static final String doubleField =
      FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(
          FieldPath.newRoot("f"), Optional.empty());
  private static final String uuidField =
      FieldName.TypeField.UUID.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());
  private static final String nullField =
      FieldName.TypeField.NULL.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());
  private static final String objectIdField =
      FieldName.TypeField.OBJECT_ID.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());
  private static final String booleanField =
      FieldName.TypeField.BOOLEAN.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());

  /** Sets up test. */
  @Before
  public void setup() throws IOException {
    this.directory = new ByteBuffersDirectory();

    List<Document> docs = new ArrayList<>();
    Document doc0 = new Document();
    doc0.add(new SortedSetDocValuesField(stringField, new BytesRef("1")));
    docs.add(doc0);

    Document doc1 = new Document();
    doc1.add(
        new SortedNumericDocValuesField(
            doubleField, LuceneDoubleConversionUtils.toMqlSortableLong(0.0)));
    docs.add(doc1);

    Document doc2 = new Document();
    doc2.add(new SortedNumericDocValuesField(dateField, 123456789));
    docs.add(doc2);

    Document doc3 = new Document();
    doc3.add(new SortedNumericDocValuesField(intField, 1));
    docs.add(doc3);

    Document doc4 = new Document();
    doc4.add(new SortedSetDocValuesField(stringField, new BytesRef("2")));
    docs.add(doc4);

    Document doc5 = new Document();
    doc5.add(
        new SortedNumericDocValuesField(
            doubleField, LuceneDoubleConversionUtils.toMqlSortableLong(2.0)));
    docs.add(doc5);

    Document doc6 = new Document();
    doc6.add(new SortedNumericDocValuesField(dateField, 923456789));
    docs.add(doc6);

    Document doc7 = new Document();
    doc7.add(new SortedNumericDocValuesField(intField, 3));
    docs.add(doc7);

    Document doc8 = new Document();
    doc8.add(new SortedNumericDocValuesField(dateField, Integer.MAX_VALUE));
    docs.add(doc8);

    Document doc9 = new Document();
    docs.add(doc9);

    Document doc10 = new Document();
    doc10.add(
        new SortedNumericDocValuesField(
            doubleField, LuceneDoubleConversionUtils.toMqlSortableLong(Double.NaN)));
    docs.add(doc10);

    Document doc11 = new Document();
    doc11.add(
        new SortedSetDocValuesField(
            uuidField, new BytesRef("11111111-1111-1111-1111-111111111111")));
    docs.add(doc11);

    Document doc12 = new Document();
    doc12.add(
        new SortedSetDocValuesField(
            uuidField, new BytesRef("22222222-2222-2222-2222-222222222222")));
    docs.add(doc12);

    Document doc13 = new Document();
    doc13.add(new SortedDocValuesField(nullField, new BytesRef(FieldValue.NULL_FIELD_VALUE)));
    docs.add(doc13);

    Document doc14 = new Document();
    doc14.add(
        new SortedSetDocValuesField(
            objectIdField, new BytesRef(new ObjectId("7".repeat(24)).toByteArray())));
    docs.add(doc14);

    Document doc15 = new Document();
    doc15.add(
        new SortedSetDocValuesField(
            objectIdField, new BytesRef(new ObjectId("F".repeat(24)).toByteArray())));
    docs.add(doc15);

    Document doc16 = new Document();
    doc16.add(
        new SortedSetDocValuesField(
            booleanField, new BytesRef(FieldValue.BOOLEAN_FALSE_FIELD_VALUE)));
    docs.add(doc16);

    Document doc17 = new Document();
    doc17.add(
        new SortedSetDocValuesField(
            booleanField, new BytesRef(FieldValue.BOOLEAN_TRUE_FIELD_VALUE)));
    docs.add(doc17);

    try (var writer = new RandomSegmentingIndexWriter(this.directory)) {
      writer.addDocuments(docs);
    }

    DirectoryReader reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(reader);
  }

  @After
  public void cleanup() throws IOException {
    this.searcher.getIndexReader().close();
    this.directory.close();
  }

  /** Create a single column sort over field 'f' with the given SortOptions. */
  private static Sort createSort(SortOptions options) {
    SortField[] sortFields = new SortField[1];
    sortFields[0] =
        new MqlMixedSort(new MongotSortField(FieldPath.newRoot("f"), options), Optional.empty());
    return new Sort(sortFields);
  }

  @Test
  public void testCompareBottom() throws IOException {
    Query query = new MatchAllDocsQuery();
    Sort sort = createSort(UserFieldSortOptions.DEFAULT_ASC);

    TopDocs topDocs = this.searcher.search(query, 16, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

    assertThat(values)
        .asList()
        .containsExactly(
            BsonUtils.MIN_KEY,
            BsonUtils.MIN_KEY,
            new BsonDouble(Double.NaN),
            new BsonDouble(0.0),
            new BsonInt64(1),
            new BsonDouble(2.0),
            new BsonInt64(3),
            new BsonString("1"),
            new BsonString("2"),
            new BsonBinary(UUID.fromString("11111111-1111-1111-1111-111111111111")),
            new BsonBinary(UUID.fromString("22222222-2222-2222-2222-222222222222")),
            new BsonObjectId(new ObjectId("7".repeat(24))),
            new BsonObjectId(new ObjectId("F".repeat(24))),
            BsonBoolean.FALSE,
            BsonBoolean.TRUE,
            new BsonDateTime(123456789))
        .inOrder();
  }

  @Test
  public void testCompareTop() throws IOException {
    Sort sort = createSort(UserFieldSortOptions.DEFAULT_ASC);
    Query query = new MatchAllDocsQuery();

    ScoreDoc after = new FieldDoc(-1, 1f, new Object[] {new BsonDouble(0.0)});
    TopDocs topDocs = this.searcher.searchAfter(after, query, 15, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

    assertThat(values)
        .asList()
        .containsExactly(
            new BsonDouble(0.0),
            new BsonInt64(1),
            new BsonDouble(2.0),
            new BsonInt64(3),
            new BsonString("1"),
            new BsonString("2"),
            new BsonBinary(UUID.fromString("11111111-1111-1111-1111-111111111111")),
            new BsonBinary(UUID.fromString("22222222-2222-2222-2222-222222222222")),
            new BsonObjectId(new ObjectId("7".repeat(24))),
            new BsonObjectId(new ObjectId("F".repeat(24))),
            BsonBoolean.FALSE,
            BsonBoolean.TRUE,
            new BsonDateTime(123456789),
            new BsonDateTime(923456789),
            new BsonDateTime(Integer.MAX_VALUE))
        .inOrder();
  }

  @Test
  public void testCompareBottomDesc() throws IOException {
    Sort sort = createSort(UserFieldSortOptions.DEFAULT_DESC);
    Query query = new MatchAllDocsQuery();

    TopDocs topDocs = this.searcher.search(query, 18, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

    assertThat(values)
        .asList()
        .containsExactly(
            new BsonDateTime(Integer.MAX_VALUE),
            new BsonDateTime(923456789),
            new BsonDateTime(123456789),
            BsonBoolean.TRUE,
            BsonBoolean.FALSE,
            new BsonObjectId(new ObjectId("F".repeat(24))),
            new BsonObjectId(new ObjectId("7".repeat(24))),
            new BsonBinary(UUID.fromString("22222222-2222-2222-2222-222222222222")),
            new BsonBinary(UUID.fromString("11111111-1111-1111-1111-111111111111")),
            new BsonString("2"),
            new BsonString("1"),
            new BsonInt64(3),
            new BsonDouble(2.0),
            new BsonInt64(1),
            new BsonDouble(0.0),
            new BsonDouble(Double.NaN),
            BsonUtils.MIN_KEY,
            BsonUtils.MIN_KEY)
        .inOrder();
  }

  @Test
  public void testCompareTopDesc() throws IOException {
    Sort sort = createSort(UserFieldSortOptions.DEFAULT_DESC);
    Query query = new MatchAllDocsQuery();
    ScoreDoc after = new FieldDoc(-1, 1f, new Object[] {new BsonDateTime(123456789)});

    TopDocs topDocs = this.searcher.searchAfter(after, query, 16, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

    assertThat(values)
        .asList()
        .containsExactly(
            new BsonDateTime(123456789),
            BsonBoolean.TRUE,
            BsonBoolean.FALSE,
            new BsonObjectId(new ObjectId("F".repeat(24))),
            new BsonObjectId(new ObjectId("7".repeat(24))),
            new BsonBinary(UUID.fromString("22222222-2222-2222-2222-222222222222")),
            new BsonBinary(UUID.fromString("11111111-1111-1111-1111-111111111111")),
            new BsonString("2"),
            new BsonString("1"),
            new BsonInt64(3),
            new BsonDouble(2.0),
            new BsonInt64(1),
            new BsonDouble(0.0),
            new BsonDouble(Double.NaN),
            BsonUtils.MIN_KEY,
            BsonUtils.MIN_KEY)
        .inOrder();
  }

  @Test
  public void testGetComparableProviders_ranksAcrossSegmentsCorrectly() throws IOException {
    MqlMixedSort sort =
        new MqlMixedSort(
            new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC),
            Optional.empty());
    IndexSorter indexSorter = sort.getIndexSorter();
    assertThat(indexSorter).isNotNull();

    try (Directory dir = new ByteBuffersDirectory()) {
      // Segment 1: string "hello"
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
        Document doc0 = new Document();
        doc0.add(new SortedSetDocValuesField(stringField, new BytesRef("hello")));
        w.addDocument(doc0);
        w.commit();
      }

      // Segment 2: int 5, int 20
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
        Document doc0 = new Document();
        doc0.add(new SortedNumericDocValuesField(intField, 5));
        w.addDocument(doc0);

        Document doc1 = new Document();
        doc1.add(new SortedNumericDocValuesField(intField, 20));
        w.addDocument(doc1);
        w.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertThat(reader.leaves()).hasSize(2);

        List<LeafReader> leafReaders =
            reader.leaves().stream().map(ctx -> ctx.reader()).collect(Collectors.toList());

        IndexSorter.ComparableProvider[] providers =
            indexSorter.getComparableProviders(leafReaders);
        assertThat(providers).hasLength(2);

        // seg0 doc0 = string "hello", seg1 doc0 = int 5, seg1 doc1 = int 20
        // BSON order: int 5 < int 20 < string "hello"
        long rankString = providers[0].getAsComparableLong(0);
        long rankInt5 = providers[1].getAsComparableLong(0);
        long rankInt20 = providers[1].getAsComparableLong(1);

        assertThat(rankInt5).isLessThan(rankInt20);
        assertThat(rankInt20).isLessThan(rankString);
      }
    }
  }

  @Test
  public void testGetDocComparator_descendingOrder() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        Document doc0 = new Document();
        doc0.add(new SortedNumericDocValuesField(intField, 10));
        writer.addDocument(doc0);

        Document doc1 = new Document();
        doc1.add(new SortedSetDocValuesField(stringField, new BytesRef("hello")));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.add(new SortedNumericDocValuesField(intField, 5));
        writer.addDocument(doc2);

        writer.forceMerge(1);
      }

      MqlMixedSort sort =
          new MqlMixedSort(
              new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_DESC),
              Optional.empty());
      IndexSorter indexSorter = sort.getIndexSorter();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        IndexSorter.DocComparator docComparator =
            indexSorter.getDocComparator(leafReader, leafReader.maxDoc());

        // Descending: string "hello" > int 10 > int 5
        assertThat(docComparator.compare(1, 0)).isLessThan(0); // hello before 10
        assertThat(docComparator.compare(0, 2)).isLessThan(0); // 10 before 5
        assertThat(docComparator.compare(1, 2)).isLessThan(0); // hello before 5
      }
    }
  }

  @Test
  public void testGetComparableProviders_descendingOrder() throws IOException {
    MqlMixedSort sort =
        new MqlMixedSort(
            new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_DESC),
            Optional.empty());
    IndexSorter indexSorter = sort.getIndexSorter();

    try (Directory dir = new ByteBuffersDirectory()) {
      // Segment 1: string "hello" (single doc, trivially sorted)
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
        Document doc0 = new Document();
        doc0.add(new SortedSetDocValuesField(stringField, new BytesRef("hello")));
        w.addDocument(doc0);
        w.commit();
      }

      // Segment 2: docs in descending sort order (int 20 before int 5) to match how a
      // sorted index stores them. getComparableProviders uses a K-way merge that relies on
      // within-segment docID order matching the configured sort direction.
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
        Document doc0 = new Document();
        doc0.add(new SortedNumericDocValuesField(intField, 20));
        w.addDocument(doc0);

        Document doc1 = new Document();
        doc1.add(new SortedNumericDocValuesField(intField, 5));
        w.addDocument(doc1);
        w.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        List<LeafReader> leafReaders =
            reader.leaves().stream().map(ctx -> ctx.reader()).collect(Collectors.toList());

        IndexSorter.ComparableProvider[] providers =
            indexSorter.getComparableProviders(leafReaders);

        // Descending BSON order: string "hello" > int 20 > int 5
        long rankString = providers[0].getAsComparableLong(0);
        long rankInt20 = providers[1].getAsComparableLong(0);
        long rankInt5 = providers[1].getAsComparableLong(1);

        assertThat(rankString).isLessThan(rankInt20);
        assertThat(rankInt20).isLessThan(rankInt5);
      }
    }
  }

  @Test
  public void testEquals_differentFieldName_returnsFalse() {
    MqlMixedSort mixed =
        new MqlMixedSort(
            new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC),
            Optional.empty());

    SortField other = new SortField("g", SortField.Type.CUSTOM, false);
    assertThat(mixed.equals(other)).isFalse();
  }

  @Test
  public void testEquals_differentReverse_returnsFalse() {
    MqlMixedSort mixed =
        new MqlMixedSort(
            new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC),
            Optional.empty());

    SortField other = new SortField("f", SortField.Type.CUSTOM, true);
    assertThat(mixed.equals(other)).isFalse();
  }

  @Test
  public void testEquals_differentType_returnsFalse() {
    MqlMixedSort mixed =
        new MqlMixedSort(
            new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC),
            Optional.empty());

    SortField other = new SortField("f", SortField.Type.LONG, false);
    assertThat(mixed.equals(other)).isFalse();
  }

  @Test
  public void testComputeDocValues_multiTypeOverlap_selectsMin() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        // Doc 0: has both int (100) and string ("hello"). MIN selector picks the numerically
        // smaller value (int 100 has bracket priority 30 < string priority 60).
        Document doc0 = new Document();
        doc0.add(new SortedNumericDocValuesField(intField, 100));
        doc0.add(new SortedSetDocValuesField(stringField, new BytesRef("hello")));
        writer.addDocument(doc0);

        // Doc 1: only string ("aaa")
        Document doc1 = new Document();
        doc1.add(new SortedSetDocValuesField(stringField, new BytesRef("aaa")));
        writer.addDocument(doc1);

        writer.forceMerge(1);
      }

      MqlMixedSort sort =
          new MqlMixedSort(
              new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      IndexSorter indexSorter = sort.getIndexSorter();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        IndexSorter.DocComparator docComparator =
            indexSorter.getDocComparator(leafReader, leafReader.maxDoc());

        // doc0 should use int 100 (bracket 30), doc1 is string "aaa" (bracket 60)
        // numbers < strings in BSON order, so doc0 < doc1
        assertThat(docComparator.compare(0, 1)).isLessThan(0);
      }
    }
  }

  @Test
  public void testComputeDocValues_multiTypeOverlap_selectsMax() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        // Doc 0: has both int (100) and string ("hello"). MAX selector (from DEFAULT_DESC)
        // picks string("hello") because strings > numbers in BSON order.
        Document doc0 = new Document();
        doc0.add(new SortedNumericDocValuesField(intField, 100));
        doc0.add(new SortedSetDocValuesField(stringField, new BytesRef("hello")));
        writer.addDocument(doc0);

        // Doc 1: only int (200)
        Document doc1 = new Document();
        doc1.add(new SortedNumericDocValuesField(intField, 200));
        writer.addDocument(doc1);

        writer.forceMerge(1);
      }

      MqlMixedSort sort =
          new MqlMixedSort(
              new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_DESC),
              Optional.empty());
      IndexSorter indexSorter = sort.getIndexSorter();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        IndexSorter.DocComparator docComparator =
            indexSorter.getDocComparator(leafReader, leafReader.maxDoc());

        // With MAX selector: doc0 resolves to string "hello" (not int 100).
        // Descending comparator: string "hello" > int 200, so doc0 sorts before doc1.
        assertThat(docComparator.compare(0, 1)).isLessThan(0);
      }
    }
  }

  @Test
  public void testGetDocComparator_missingValueSortsToExpectedPosition() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        // Doc 0: has int value
        Document doc0 = new Document();
        doc0.add(new SortedNumericDocValuesField(intField, 42));
        writer.addDocument(doc0);

        // Doc 1: no value for any sort field — triggers Arrays.fill(missingValue) default
        writer.addDocument(new Document());

        // Doc 2: has string value
        Document doc2 = new Document();
        doc2.add(new SortedSetDocValuesField(stringField, new BytesRef("zzz")));
        writer.addDocument(doc2);

        writer.forceMerge(1);
      }

      MqlMixedSort sort =
          new MqlMixedSort(
              new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      IndexSorter indexSorter = sort.getIndexSorter();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        IndexSorter.DocComparator docComparator =
            indexSorter.getDocComparator(leafReader, leafReader.maxDoc());

        // DEFAULT_ASC uses LOWEST → missing gets MIN_KEY, which sorts before everything.
        // Expected ascending order: doc1 (missing) < doc0 (int 42) < doc2 (string "zzz")
        assertThat(docComparator.compare(1, 0)).isLessThan(0);
        assertThat(docComparator.compare(0, 2)).isLessThan(0);
        assertThat(docComparator.compare(1, 2)).isLessThan(0);
      }
    }
  }

  @Test
  public void testGetComparableProviders_missingValueAcrossSegments() throws IOException {
    MqlMixedSort sort =
        new MqlMixedSort(
            new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC),
            Optional.empty());
    IndexSorter indexSorter = sort.getIndexSorter();

    try (Directory dir = new ByteBuffersDirectory()) {
      // Segment 1: doc with no sort value (missing)
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
        w.addDocument(new Document());
        w.commit();
      }

      // Segment 2: int 10
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
        Document doc0 = new Document();
        doc0.add(new SortedNumericDocValuesField(intField, 10));
        w.addDocument(doc0);
        w.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertThat(reader.leaves()).hasSize(2);
        List<LeafReader> leafReaders =
            reader.leaves().stream().map(ctx -> ctx.reader()).collect(Collectors.toList());

        IndexSorter.ComparableProvider[] providers =
            indexSorter.getComparableProviders(leafReaders);

        long rankMissing = providers[0].getAsComparableLong(0);
        long rankInt10 = providers[1].getAsComparableLong(0);

        // Missing (MIN_KEY) should have a lower rank than int 10
        assertThat(rankMissing).isLessThan(rankInt10);
      }
    }
  }

  @Test
  public void testGetDocComparator_allTypeFields() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        // BSON ascending order: numbers < strings < uuid < objectId < booleans < dates
        // Doc 0: double 1.5
        Document doc0 = new Document();
        doc0.add(
            new SortedNumericDocValuesField(
                doubleField, LuceneDoubleConversionUtils.toMqlSortableLong(1.5)));
        writer.addDocument(doc0);

        // Doc 1: date 100000
        Document doc1 = new Document();
        doc1.add(new SortedNumericDocValuesField(dateField, 100000));
        writer.addDocument(doc1);

        // Doc 2: boolean false
        Document doc2 = new Document();
        doc2.add(
            new SortedSetDocValuesField(
                booleanField, new BytesRef(FieldValue.BOOLEAN_FALSE_FIELD_VALUE)));
        writer.addDocument(doc2);

        // Doc 3: null
        Document doc3 = new Document();
        doc3.add(new SortedDocValuesField(nullField, new BytesRef(FieldValue.NULL_FIELD_VALUE)));
        writer.addDocument(doc3);

        // Doc 4: string "abc"
        Document doc4 = new Document();
        doc4.add(new SortedSetDocValuesField(stringField, new BytesRef("abc")));
        writer.addDocument(doc4);

        writer.forceMerge(1);
      }

      MqlMixedSort sort =
          new MqlMixedSort(
              new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC),
              Optional.empty());
      IndexSorter indexSorter = sort.getIndexSorter();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        IndexSorter.DocComparator cmp =
            indexSorter.getDocComparator(leafReader, leafReader.maxDoc());

        // BSON ascending: null < double 1.5 < string "abc" < boolean false < date 100000
        // (null maps to MIN_KEY with LOWEST position, so sorts first)
        assertThat(cmp.compare(3, 0)).isLessThan(0); // null < double
        assertThat(cmp.compare(0, 4)).isLessThan(0); // double < string
        assertThat(cmp.compare(4, 2)).isLessThan(0); // string < boolean
        assertThat(cmp.compare(2, 1)).isLessThan(0); // boolean < date
      }
    }
  }
}
