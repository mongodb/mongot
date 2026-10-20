package com.xgen.mongot.index.lucene.query.sort.mixed;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.sort.comparator.ScopeFilters;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ScopedMqlMixedSortTest {
  private static final FieldPath PATH = FieldPath.parse("variants.val");
  private static final FieldPath SCOPE = FieldPath.parse("variants");
  private static final TermQuery ROOTS = new TermQuery(new Term("root", "true"));
  private static final BitSetProducer PARENTS = new QueryBitSetProducer(ROOTS);
  private static final BitSetProducer CHILDREN =
      new QueryBitSetProducer(new TermQuery(new Term("selected", "true")));
  private static final ScopeFilters FILTERS = new ScopeFilters(PARENTS, CHILDREN);
  private static final UUID UUID_VALUE = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private static final ObjectId OBJECT_ID = new ObjectId("7".repeat(24));

  private Directory directory;
  private IndexSearcher searcher;

  @Before
  public void setUp() throws IOException {
    this.directory = new ByteBuffersDirectory();
    try (var writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
      writer.addDocuments(
          List.of(child(number(30)), child(token("aaa")), child(token("zzz")), root("0")));
      writer.addDocuments(List.of(child(token("bbb")), child(token("ccc")), root("1")));
      writer.addDocuments(List.of(child(number(5)), root("2")));
      writer.addDocuments(
          List.of(
              child(ordinal(TypeField.BOOLEAN, new BytesRef(FieldValue.BOOLEAN_FALSE_FIELD_VALUE))),
              child(ordinal(TypeField.BOOLEAN, new BytesRef(FieldValue.BOOLEAN_TRUE_FIELD_VALUE))),
              root("3")));
      writer.addDocuments(
          List.of(
              child(ordinal(TypeField.UUID, new BytesRef(UUID_VALUE.toString()))),
              child(ordinal(TypeField.OBJECT_ID, new BytesRef(OBJECT_ID.toByteArray()))),
              root("4")));
      writer.addDocuments(
          List.of(
              child(new SortedNumericDocValuesField(fieldName(TypeField.DATE_V2), 1000)),
              child(new SortedNumericDocValuesField(fieldName(TypeField.DATE_V2), 2000)),
              root("5")));
      writer.addDocuments(List.of(child(), child(), root("6")));
      Document unmatched = new Document();
      unmatched.add(token("zzzz"));
      writer.addDocuments(List.of(unmatched, root("7")));
      writer.forceMerge(1);
    }
    this.searcher = new IndexSearcher(DirectoryReader.open(this.directory));
  }

  @After
  public void tearDown() throws IOException {
    this.searcher.getIndexReader().close();
    this.directory.close();
  }

  @Test
  public void search_ascending_combinesTypeReductions() throws IOException {
    TopDocs result = this.searcher.search(ROOTS, 20, sort(UserFieldSortOptions.DEFAULT_ASC));

    assertThat(ids(result)).containsExactly("6", "7", "2", "0", "1", "4", "3", "5").inOrder();
    assertThat(values(result))
        .containsExactly(
            BsonUtils.MIN_KEY,
            BsonUtils.MIN_KEY,
            new BsonInt64(5),
            new BsonInt64(30),
            new BsonString("bbb"),
            new BsonBinary(UUID_VALUE),
            BsonBoolean.FALSE,
            new BsonDateTime(1000))
        .inOrder();
  }

  @Test
  public void search_descending_combinesTypeReductions() throws IOException {
    TopDocs result = this.searcher.search(ROOTS, 20, sort(UserFieldSortOptions.DEFAULT_DESC));

    assertThat(ids(result)).containsExactly("5", "3", "4", "0", "1", "2", "6", "7").inOrder();
    assertThat(values(result))
        .containsExactly(
            new BsonDateTime(2000),
            BsonBoolean.TRUE,
            new BsonObjectId(OBJECT_ID),
            new BsonString("zzz"),
            new BsonString("ccc"),
            new BsonInt64(5),
            BsonUtils.MIN_KEY,
            BsonUtils.MIN_KEY)
        .inOrder();
  }

  @Test
  public void topK_repeatedOrdinalRead_keepsBestRoot() throws IOException {
    TopDocs result = this.searcher.search(ROOTS, 1, sort(UserFieldSortOptions.DEFAULT_DESC));

    assertThat(ids(result)).containsExactly("5");
    assertThat(values(result)).containsExactly(new BsonDateTime(2000));
  }

  @Test
  public void searchAfter_repeatedOrdinalReads_preservesNextPage() throws IOException {
    Sort sort = sort(UserFieldSortOptions.DEFAULT_ASC);
    TopDocs first = this.searcher.search(ROOTS, 4, sort);

    TopDocs next = this.searcher.searchAfter(first.scoreDocs[3], ROOTS, 4, sort);

    assertThat(ids(next)).containsExactly("1", "4", "3", "5").inOrder();
    assertThat(values(next))
        .containsExactly(
            new BsonString("bbb"),
            new BsonBinary(UUID_VALUE),
            BsonBoolean.FALSE,
            new BsonDateTime(1000))
        .inOrder();
  }

  @Test
  public void searchAfter_missingRoot_preservesMissingAndValuedRoots() throws IOException {
    Sort sort = sort(UserFieldSortOptions.DEFAULT_ASC);
    TopDocs first = this.searcher.search(ROOTS, 1, sort);

    TopDocs next = this.searcher.searchAfter(first.scoreDocs[0], ROOTS, 20, sort);

    assertThat(ids(next)).containsExactly("7", "2", "0", "1", "4", "3", "5").inOrder();
    assertThat(values(next).getFirst()).isEqualTo(BsonUtils.MIN_KEY);
  }

  @Test
  public void equals_scopedAndUnscoped_areDistinctInBothDirections() {
    MongotSortField field = new MongotSortField(PATH, UserFieldSortOptions.DEFAULT_ASC);
    MqlMixedSort scoped = MqlMixedSort.scoped(field, SCOPE, FILTERS);
    MqlMixedSort unscoped = new MqlMixedSort(field, Optional.empty());
    SortField serialized = new SortField(PATH.toString(), SortField.Type.CUSTOM, false);

    assertThat(scoped.equals(unscoped)).isFalse();
    assertThat(unscoped.equals(scoped)).isFalse();
    assertThat(scoped.equals(serialized)).isFalse();
    assertThat(serialized.equals(scoped)).isFalse();
    assertThat(unscoped).isEqualTo(serialized);
    assertThat(unscoped.hashCode()).isEqualTo(serialized.hashCode());
  }

  @Test
  public void equals_scopedSorts_includeTheirFilters() {
    MongotSortField field = new MongotSortField(PATH, UserFieldSortOptions.DEFAULT_ASC);
    MqlMixedSort first = MqlMixedSort.scoped(field, SCOPE, FILTERS);
    MqlMixedSort same = MqlMixedSort.scoped(field, SCOPE, FILTERS);
    MqlMixedSort different =
        MqlMixedSort.scoped(
            field,
            SCOPE,
            new ScopeFilters(
                PARENTS, new QueryBitSetProducer(new TermQuery(new Term("selected", "false")))));

    assertThat(first).isEqualTo(same);
    assertThat(first.hashCode()).isEqualTo(same.hashCode());
    assertThat(first).isNotEqualTo(different);
  }

  @Test
  public void topK_unscopedIndexOrder_doesNotEarlyTerminate() throws IOException {
    try (Directory index = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(index, new IndexWriterConfig())) {
      // Physical root order is 1, 2, 3; scoped keys are 30, 40, 10. Incorrect early termination
      // stops at the noncompetitive second root and misses the winning third root.
      writer.addDocuments(List.of(child(number(30)), rootWithUnscopedValue("0", 1)));
      writer.addDocuments(List.of(child(number(40)), rootWithUnscopedValue("1", 2)));
      writer.addDocuments(List.of(child(number(10)), rootWithUnscopedValue("2", 3)));
      try (DirectoryReader reader = DirectoryReader.open(writer);
          LeafReader ordered = withUnscopedIndexOrder(reader.leaves().getFirst().reader())) {
        IndexSearcher searcher = new IndexSearcher(ordered);
        Sort unscoped =
            new Sort(
                new MqlMixedSort(
                    new MongotSortField(PATH, UserFieldSortOptions.DEFAULT_ASC), Optional.empty()));

        TopDocs physicalFirst =
            searcher.search(ROOTS, new TopFieldCollectorManager(unscoped, 1, null, 0));
        TopDocs scopedFirst =
            searcher.search(
                ROOTS,
                new TopFieldCollectorManager(sort(UserFieldSortOptions.DEFAULT_ASC), 1, null, 0));

        assertThat(ids(searcher, physicalFirst)).containsExactly("0");
        assertThat(values(physicalFirst)).containsExactly(new BsonInt64(1));
        assertThat(ids(searcher, scopedFirst)).containsExactly("2");
        assertThat(values(scopedFirst)).containsExactly(new BsonInt64(10));
      }
    }
  }

  private static LeafReader withUnscopedIndexOrder(LeafReader leaf) {
    leaf.incRef();
    // Simulate its serialized metadata without involving index-sort codec persistence.
    return new FilterLeafReader(leaf) {
      @Override
      public LeafMetaData getMetaData() {
        LeafMetaData metadata = this.in.getMetaData();
        return new LeafMetaData(
            metadata.createdVersionMajor(),
            metadata.minVersion(),
            new Sort(new SortField(PATH.toString(), SortField.Type.CUSTOM, false)),
            metadata.hasBlocks());
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return this.in.getCoreCacheHelper();
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }

  @Test
  public void getIndexSorter_scopedSort_isUnavailable() {
    assertThat(sort(UserFieldSortOptions.DEFAULT_ASC).getSort()[0].getIndexSorter()).isNull();
  }

  private static Sort sort(UserFieldSortOptions options) {
    return new Sort(MqlMixedSort.scoped(new MongotSortField(PATH, options), SCOPE, FILTERS));
  }

  private List<String> ids(TopDocs result) throws IOException {
    return ids(this.searcher, result);
  }

  private static List<String> ids(IndexSearcher searcher, TopDocs result) throws IOException {
    List<String> ids = new ArrayList<>();
    for (var hit : result.scoreDocs) {
      ids.add(searcher.storedFields().document(hit.doc).get("id"));
    }
    return ids;
  }

  private static List<BsonValue> values(TopDocs result) {
    return Arrays.stream(result.scoreDocs)
        .map(hit -> (BsonValue) ((FieldDoc) hit).fields[0])
        .toList();
  }

  private static String fieldName(TypeField type) {
    return type.getLuceneFieldName(PATH, Optional.of(SCOPE));
  }

  private static Field number(long value) {
    return new SortedNumericDocValuesField(fieldName(TypeField.NUMBER_INT64_V2), value);
  }

  private static Field token(String value) {
    return ordinal(TypeField.TOKEN, new BytesRef(value));
  }

  private static Field ordinal(TypeField type, BytesRef value) {
    return new SortedSetDocValuesField(fieldName(type), value);
  }

  private static Document child(Field... values) {
    Document child = new Document();
    child.add(new StringField("selected", "true", Field.Store.NO));
    for (Field value : values) {
      child.add(value);
    }
    return child;
  }

  private static Document root(String id) {
    Document root = new Document();
    root.add(new StringField("root", "true", Field.Store.NO));
    root.add(new StoredField("id", id));
    return root;
  }

  private static Document rootWithUnscopedValue(String id, long value) {
    Document root = root(id);
    root.add(
        new SortedNumericDocValuesField(
            TypeField.NUMBER_INT64_V2.getLuceneFieldName(PATH, Optional.empty()), value));
    return root;
  }
}
