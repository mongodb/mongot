package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EmbeddedScopedTokenSortTest {
  private static final FieldPath SCOPE = FieldPath.parse("variants");
  private static final FieldPath PATH = FieldPath.parse("variants.category");
  private static final String FIELD =
      FieldName.TypeField.TOKEN.getLuceneFieldName(PATH, Optional.of(SCOPE));
  private static final TermQuery ROOTS = new TermQuery(new Term("docType", "root"));

  private final Sort sort;
  private final List<String> expectedIds;
  private final List<BsonValue> expectedValues;
  private Directory directory;
  private IndexSearcher searcher;

  public EmbeddedScopedTokenSortTest(
      SortOrder order,
      NullEmptySortPosition nullPosition,
      List<String> expectedIds,
      List<BsonValue> expectedValues) {
    var options =
        new UserFieldSortOptions(
            order,
            order == SortOrder.ASC ? SortSelector.MIN : SortSelector.MAX,
            nullPosition,
            Optional.empty());
    this.sort =
        new Sort(
            MqlSortedSetSortField.scopedTokenSort(
                new MongotSortField(PATH, options),
                SCOPE,
                new QueryBitSetProducer(ROOTS),
                new QueryBitSetProducer(new TermQuery(new Term("inStock", "true")))));
    this.expectedIds = expectedIds;
    this.expectedValues = expectedValues;
  }

  @Parameterized.Parameters(name = "order={0}, noData={1}")
  public static List<Object[]> data() {
    return List.of(
        new Object[] {
          SortOrder.ASC,
          NullEmptySortPosition.LOWEST,
          List.of("3", "4", "1", "0", "2"),
          List.of(
              BsonUtils.MIN_KEY,
              BsonUtils.MIN_KEY,
              new BsonString("b"),
              new BsonString("m"),
              new BsonString("p"))
        },
        new Object[] {
          SortOrder.DESC,
          NullEmptySortPosition.LOWEST,
          List.of("1", "2", "0", "3", "4"),
          List.of(
              new BsonString("z"),
              new BsonString("q"),
              new BsonString("n"),
              BsonUtils.MIN_KEY,
              BsonUtils.MIN_KEY)
        },
        new Object[] {
          SortOrder.ASC,
          NullEmptySortPosition.HIGHEST,
          List.of("1", "0", "2", "3", "4"),
          List.of(
              new BsonString("b"),
              new BsonString("m"),
              new BsonString("p"),
              BsonUtils.MAX_KEY,
              BsonUtils.MAX_KEY)
        },
        new Object[] {
          SortOrder.DESC,
          NullEmptySortPosition.HIGHEST,
          List.of("3", "4", "1", "2", "0"),
          List.of(
              BsonUtils.MAX_KEY,
              BsonUtils.MAX_KEY,
              new BsonString("z"),
              new BsonString("q"),
              new BsonString("n"))
        });
  }

  @Before
  public void setUp() throws IOException {
    this.directory = new ByteBuffersDirectory();
    try (var writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
      writer.addDocuments(List.of(child(true, "m"), child(true, "n"), root("0")));
      // This root replaces the first hit for both MIN and MAX, forcing compareBottom then copy.
      writer.addDocuments(
          List.of(child(true, "b", "y"), child(true, "z"), child(false, "a", "zz"), root("1")));
      writer.addDocuments(List.of(child(true, "p", "q"), root("2")));
      writer.addDocuments(List.of(child(true), child(true), root("3")));
      writer.addDocuments(List.of(child(false, "a", "zz"), root("4")));
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
  public void search_matchingChildren_reducesOrdinalsAndPreservesMissingValues()
      throws IOException {
    Result result = readResults(this.searcher.search(ROOTS, 5, this.sort));

    assertThat(result.ids()).containsExactlyElementsIn(this.expectedIds).inOrder();
    assertThat(result.values()).containsExactlyElementsIn(this.expectedValues).inOrder();
  }

  @Test
  public void topK_repeatedParentRead_preservesReduction() throws IOException {
    Result result = readResults(this.searcher.search(ROOTS, 1, this.sort));

    assertThat(result.ids()).containsExactly(this.expectedIds.getFirst());
    assertThat(result.values()).containsExactly(this.expectedValues.getFirst());
  }

  @Test
  public void searchAfter_repeatedParentReads_preservesRemainingResults() throws IOException {
    // Collect every hit first so this test isolates compareTop/copy from top-K replacement.
    TopDocs all = this.searcher.search(ROOTS, 5, this.sort);

    Result result = readResults(this.searcher.searchAfter(all.scoreDocs[0], ROOTS, 5, this.sort));

    assertThat(result.ids()).containsExactlyElementsIn(this.expectedIds.subList(1, 5)).inOrder();
    assertThat(result.values())
        .containsExactlyElementsIn(this.expectedValues.subList(1, 5))
        .inOrder();
  }

  private Result readResults(TopDocs hits) throws IOException {
    List<String> ids = new ArrayList<>();
    List<BsonValue> values = new ArrayList<>();
    for (var hit : hits.scoreDocs) {
      ids.add(this.searcher.storedFields().document(hit.doc).get("id"));
      values.add((BsonValue) ((FieldDoc) hit).fields[0]);
    }
    return new Result(ids, values);
  }

  private record Result(List<String> ids, List<BsonValue> values) {}

  private static Document child(boolean inStock, String... values) {
    Document child = new Document();
    child.add(new StringField("inStock", Boolean.toString(inStock), Field.Store.NO));
    for (String value : values) {
      child.add(new SortedSetDocValuesField(FIELD, new BytesRef(value)));
    }
    return child;
  }

  private static Document root(String id) {
    Document root = new Document();
    root.add(new StringField("docType", "root", Field.Store.NO));
    root.add(new StoredField("id", id));
    return root;
  }
}
