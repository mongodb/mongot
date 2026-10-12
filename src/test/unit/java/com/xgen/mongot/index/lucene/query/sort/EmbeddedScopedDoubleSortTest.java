package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
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
import org.bson.BsonDouble;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EmbeddedScopedDoubleSortTest {
  private static final FieldPath SCOPE = FieldPath.parse("variants");
  private static final FieldPath PRICE = FieldPath.parse("variants.price");
  private static final String PRICE_FIELD =
      FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(PRICE, Optional.of(SCOPE));
  private static final TermQuery ROOTS = new TermQuery(new Term("root", "true"));

  private final UserFieldSortOptions options;
  private final List<String> expectedIds;
  private final List<BsonValue> expectedValues;
  private Directory directory;
  private IndexSearcher searcher;

  public EmbeddedScopedDoubleSortTest(
      SortOrder order,
      NullEmptySortPosition noData,
      List<String> expectedIds,
      List<BsonValue> expectedValues) {
    this.options = new UserFieldSortOptions(order, noData);
    this.expectedIds = expectedIds;
    this.expectedValues = expectedValues;
  }

  @Parameterized.Parameters(name = "order={0}, noData={1}")
  public static List<Object[]> data() {
    return List.of(
        new Object[] {
          SortOrder.ASC,
          NullEmptySortPosition.LOWEST,
          List.of("0", "1", "2", "3"),
          List.of(BsonUtils.MIN_KEY, BsonUtils.MIN_KEY, new BsonDouble(-2.5), new BsonDouble(-1.5))
        },
        new Object[] {
          SortOrder.DESC,
          NullEmptySortPosition.LOWEST,
          List.of("2", "3", "0", "1"),
          List.of(new BsonDouble(3.5), new BsonDouble(2.5), BsonUtils.MIN_KEY, BsonUtils.MIN_KEY)
        },
        new Object[] {
          SortOrder.ASC,
          NullEmptySortPosition.HIGHEST,
          List.of("2", "3", "0", "1"),
          List.of(new BsonDouble(-2.5), new BsonDouble(-1.5), BsonUtils.MAX_KEY, BsonUtils.MAX_KEY)
        },
        new Object[] {
          SortOrder.DESC,
          NullEmptySortPosition.HIGHEST,
          List.of("0", "1", "2", "3"),
          List.of(BsonUtils.MAX_KEY, BsonUtils.MAX_KEY, new BsonDouble(3.5), new BsonDouble(2.5))
        });
  }

  @Before
  public void setUp() throws IOException {
    this.directory = new ByteBuffersDirectory();
    try (var writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
      writer.addDocuments(List.of(child(100, false), root("0")));
      writer.addDocuments(List.of(matchingChild(), matchingChild(), root("1")));
      writer.addDocuments(
          List.of(child(-2.5, true), child(3.5, true), child(99, false), root("2")));
      writer.addDocuments(List.of(child(-1.5, true), child(2.5, true), root("3")));
    }
    this.searcher = new IndexSearcher(DirectoryReader.open(this.directory));
  }

  @After
  public void tearDown() throws IOException {
    this.searcher.getIndexReader().close();
    this.directory.close();
  }

  @Test
  public void search_matchingChildren_reducesAndOrdersMissing() throws IOException {
    TopDocs results = this.searcher.search(ROOTS, 10, sort());

    assertThat(ids(results)).containsExactlyElementsIn(this.expectedIds).inOrder();
    assertThat(values(results)).containsExactlyElementsIn(this.expectedValues).inOrder();
  }

  @Test
  public void searchAfter_missingValues_preservesEveryRoot() throws IOException {
    Sort sort = sort();

    TopDocs first = this.searcher.search(ROOTS, 1, sort);
    TopDocs second = this.searcher.searchAfter(first.scoreDocs[0], ROOTS, 1, sort);
    TopDocs third = this.searcher.searchAfter(second.scoreDocs[0], ROOTS, 1, sort);
    TopDocs fourth = this.searcher.searchAfter(third.scoreDocs[0], ROOTS, 1, sort);

    assertThat(ids(first, second, third, fourth))
        .containsExactlyElementsIn(this.expectedIds)
        .inOrder();
    assertThat(values(first, second, third, fourth))
        .containsExactlyElementsIn(this.expectedValues)
        .inOrder();
  }

  private Sort sort() {
    return new Sort(
        new EmbeddedScopedDoubleSort(
            new MongotSortField(PRICE, this.options),
            SCOPE,
            new QueryBitSetProducer(ROOTS),
            new QueryBitSetProducer(new TermQuery(new Term("selected", "true")))));
  }

  private List<String> ids(TopDocs... pages) throws IOException {
    List<String> result = new ArrayList<>();
    for (TopDocs page : pages) {
      for (var hit : page.scoreDocs) {
        result.add(this.searcher.storedFields().document(hit.doc).get("id"));
      }
    }
    return result;
  }

  private static List<BsonValue> values(TopDocs... pages) {
    return Arrays.stream(pages)
        .flatMap(page -> Arrays.stream(page.scoreDocs))
        .map(hit -> (BsonValue) ((FieldDoc) hit).fields[0])
        .toList();
  }

  private static Document child(double price, boolean selected) {
    Document child = new Document();
    child.add(
        new SortedNumericDocValuesField(
            PRICE_FIELD, LuceneDoubleConversionUtils.toMqlSortableLong(price)));
    child.add(new StringField("selected", Boolean.toString(selected), Field.Store.NO));
    return child;
  }

  private static Document matchingChild() {
    Document child = new Document();
    child.add(new StringField("selected", "true", Field.Store.NO));
    return child;
  }

  private static Document root(String id) {
    Document root = new Document();
    root.add(new StringField("root", "true", Field.Store.NO));
    root.add(new StoredField("id", id));
    return root;
  }
}
