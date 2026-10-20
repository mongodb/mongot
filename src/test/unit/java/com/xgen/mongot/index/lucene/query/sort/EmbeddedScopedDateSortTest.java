package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.comparator.ScopeFilters;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
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
import org.bson.BsonDateTime;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EmbeddedScopedDateSortTest {
  private static final FieldPath SCOPE = FieldPath.parse("variants");
  private static final FieldPath PATH = FieldPath.parse("variants.addedOn");
  private static final String FIELD =
      FieldName.TypeField.DATE_V2.getLuceneFieldName(PATH, Optional.of(SCOPE));
  private static final TermQuery ROOTS = new TermQuery(new Term("docType", "root"));

  private final Sort sort;
  private final List<String> expectedIds;
  private final List<BsonValue> expectedValues;
  private Directory directory;
  private IndexSearcher searcher;

  public EmbeddedScopedDateSortTest(
      SortOrder order, NullEmptySortPosition nullPosition, List<String> expectedIds) {
    var options =
        new UserFieldSortOptions(
            order,
            order == SortOrder.ASC ? SortSelector.MIN : SortSelector.MAX,
            nullPosition,
            Optional.empty());
    this.sort =
        new Sort(
            new EmbeddedScopedDateSort(
                new MongotSortField(PATH, options),
                SCOPE,
                new ScopeFilters(
                    new QueryBitSetProducer(ROOTS),
                    new QueryBitSetProducer(new TermQuery(new Term("inStock", "true"))))));
    this.expectedIds = expectedIds;
    List<BsonValue> valuesById =
        List.of(
            new BsonDateTime(order == SortOrder.ASC ? -5000 : 9000),
            new BsonDateTime(order == SortOrder.ASC ? -1000 : 10000),
            nullPosition.getNullMissingSortValue(),
            nullPosition.getNullMissingSortValue(),
            new BsonDateTime(Long.MIN_VALUE),
            new BsonDateTime(Long.MAX_VALUE),
            new BsonDateTime(0));
    this.expectedValues =
        expectedIds.stream().map(id -> valuesById.get(Integer.parseInt(id))).toList();
  }

  @Parameterized.Parameters(name = "order={0}, noData={1}")
  public static List<Object[]> data() {
    return List.of(
        new Object[] {
          SortOrder.ASC, NullEmptySortPosition.LOWEST, List.of("2", "3", "4", "0", "1", "6", "5")
        },
        new Object[] {
          SortOrder.DESC, NullEmptySortPosition.LOWEST, List.of("5", "1", "0", "6", "4", "2", "3")
        },
        new Object[] {
          SortOrder.ASC, NullEmptySortPosition.HIGHEST, List.of("4", "0", "1", "6", "5", "2", "3")
        },
        new Object[] {
          SortOrder.DESC, NullEmptySortPosition.HIGHEST, List.of("2", "3", "5", "1", "0", "6", "4")
        });
  }

  @Before
  public void setUp() throws IOException {
    this.directory = new ByteBuffersDirectory();
    try (var writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
      writer.addDocuments(
          List.of(
              child(true, -5000, 2000),
              child(true, 9000),
              child(false, Long.MIN_VALUE, Long.MAX_VALUE),
              root("0")));
      writer.addDocuments(List.of(child(true, -1000), child(true, 10000), root("1")));
      writer.addDocuments(List.of(child(false, 0), root("2")));
      writer.addDocuments(List.of(child(true), child(true), root("3")));
      writer.addDocuments(List.of(child(true, Long.MIN_VALUE), root("4")));
      writer.addDocuments(List.of(child(true, Long.MAX_VALUE), root("5")));
      writer.addDocuments(List.of(child(true, 0), root("6")));
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
  public void search_matchingChildren_returnsBsonDatesAndDistinctMissingKeys() throws IOException {
    TopDocs result = this.searcher.search(ROOTS, 7, this.sort);

    assertThat(ids(result)).containsExactlyElementsIn(this.expectedIds).inOrder();
    assertThat(values(result)).containsExactlyElementsIn(this.expectedValues).inOrder();
  }

  @Test
  public void topK_competitiveParents_preservesReducedDate() throws IOException {
    TopDocs result = this.searcher.search(ROOTS, 3, this.sort);

    assertThat(ids(result)).containsExactlyElementsIn(this.expectedIds.subList(0, 3)).inOrder();
    assertThat(values(result))
        .containsExactlyElementsIn(this.expectedValues.subList(0, 3))
        .inOrder();
  }

  @Test
  public void searchAfter_firstKey_preservesRemainingDatesAndMissingKeys() throws IOException {
    TopDocs first = this.searcher.search(ROOTS, 1, this.sort);

    TopDocs next = this.searcher.searchAfter(first.scoreDocs[0], ROOTS, 7, this.sort);

    assertThat(ids(next)).containsExactlyElementsIn(this.expectedIds.subList(1, 7)).inOrder();
    assertThat(values(next)).containsExactlyElementsIn(this.expectedValues.subList(1, 7)).inOrder();
  }

  private List<String> ids(TopDocs hits) throws IOException {
    List<String> ids = new ArrayList<>();
    for (var hit : hits.scoreDocs) {
      ids.add(this.searcher.storedFields().document(hit.doc).get("id"));
    }
    return ids;
  }

  private static List<BsonValue> values(TopDocs hits) {
    return Arrays.stream(hits.scoreDocs)
        .map(hit -> (BsonValue) ((FieldDoc) hit).fields[0])
        .toList();
  }

  private static Document child(boolean inStock, long... dates) {
    Document child = new Document();
    child.add(new StringField("inStock", Boolean.toString(inStock), Field.Store.NO));
    for (long date : dates) {
      child.add(new SortedNumericDocValuesField(FIELD, date));
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
