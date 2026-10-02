package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies {@link EmbeddedScopedLongSort} reduces the sort key over only a root's matching children
 * (MIN ascending / MAX descending), places roots with no matching-child value per {@code
 * NullEmptySortPosition}, and surfaces the reduced value (or MinKey/MaxKey) as the BSON sort value.
 *
 * <p>Index layout: each root is one block {@code [child.., root]}. Children carry {@code price} and
 * an {@code inStock} marker; the scope selects {@code inStock=true} children. Roots carry {@code
 * docType=root} and a stored {@code id}.
 */
public class EmbeddedScopedLongSortTest {

  private static final FieldPath SCOPE_PATH = FieldPath.parse("variants");
  private static final String PRICE_FIELD =
      FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
          FieldPath.parse("variants.price"), Optional.of(SCOPE_PATH));

  private Directory directory;
  private IndexSearcher searcher;

  private static Document child(long price, boolean inStock) {
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField(PRICE_FIELD, price));
    doc.add(new StringField("inStock", inStock ? "true" : "false", Field.Store.NO));
    return doc;
  }

  /** A child that matches the scope but has no value for the sort field. */
  private static Document childNoValue(boolean inStock) {
    Document doc = new Document();
    doc.add(new StringField("inStock", inStock ? "true" : "false", Field.Store.NO));
    return doc;
  }

  private static Document root(int id) {
    Document doc = new Document();
    doc.add(new StringField("docType", "root", Field.Store.NO));
    doc.add(new StoredField("id", Integer.toString(id)));
    return doc;
  }

  private static List<Document> block(int id, List<Document> children) {
    List<Document> block = new ArrayList<>(children);
    block.add(root(id));
    return block;
  }

  @Before
  public void setup() throws IOException {
    this.directory = new ByteBuffersDirectory();
    try (IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
      // id 0: matching {30, 40}, non-matching {10}  -> min 30, max 40
      writer.addDocuments(
          block(0, ImmutableList.of(child(30, true), child(40, true), child(10, false))));
      // id 1: matching {20}                          -> min 20, max 20
      writer.addDocuments(block(1, ImmutableList.of(child(20, true))));
      // id 2: matching {120}, non-matching {12}      -> min 120, max 120
      writer.addDocuments(block(2, ImmutableList.of(child(120, true), child(12, false))));
      // id 3: only non-matching children             -> missing
      writer.addDocuments(block(3, ImmutableList.of(child(5, false))));
      // id 4: matching child but no sort value        -> missing
      writer.addDocuments(block(4, ImmutableList.of(childNoValue(true))));
    }
    this.searcher = new IndexSearcher(DirectoryReader.open(this.directory));
  }

  @After
  public void cleanup() throws IOException {
    this.searcher.getIndexReader().close();
    this.directory.close();
  }

  private Result runScopedSort(UserFieldSortOptions options) throws IOException {
    TopDocs topDocs =
        this.searcher.search(new TermQuery(new Term("docType", "root")), 100, scopedSort(options));
    return readResults(topDocs);
  }

  private static Sort scopedSort(UserFieldSortOptions options) {
    BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("docType", "root")));
    BitSetProducer children = new QueryBitSetProducer(new TermQuery(new Term("inStock", "true")));

    SortField sortField =
        new EmbeddedScopedLongSort(
            new MongotSortField(FieldPath.parse("variants.price"), options),
            SCOPE_PATH,
            parents,
            children);

    return new Sort(sortField);
  }

  private Result readResults(TopDocs topDocs) throws IOException {
    StoredFields storedFields = this.searcher.storedFields();
    List<String> ids = new ArrayList<>();
    List<BsonValue> sortValues = new ArrayList<>();
    for (var scoreDoc : topDocs.scoreDocs) {
      ids.add(storedFields.document(scoreDoc.doc).get("id"));
      sortValues.add((BsonValue) ((FieldDoc) scoreDoc).fields[0]);
    }
    return new Result(ids, sortValues);
  }

  @Test
  public void ascendingReducesWithMinAndPlacesMissingLowest() throws IOException {
    Result result = runScopedSort(UserFieldSortOptions.DEFAULT_ASC);

    // Missing (ids 3, 4) sort as MinKey -> first; then 20, 30, 120.
    assertThat(result.ids.subList(2, 5)).containsExactly("1", "0", "2").inOrder();
    assertThat(result.ids.subList(0, 2)).containsExactly("3", "4");
    assertThat(result.sortValues.subList(2, 5))
        .containsExactly(new BsonInt64(20), new BsonInt64(30), new BsonInt64(120))
        .inOrder();
    assertThat(result.sortValues.subList(0, 2))
        .containsExactly(BsonUtils.MIN_KEY, BsonUtils.MIN_KEY);
  }

  @Test
  public void descendingReducesWithMaxAndPlacesMissingLowest() throws IOException {
    Result result = runScopedSort(UserFieldSortOptions.DEFAULT_DESC);

    // MAX reduction: id 0 -> 40. Descending, nulls lowest -> missing (3, 4) last.
    assertThat(result.ids.subList(0, 3)).containsExactly("2", "0", "1").inOrder();
    assertThat(result.ids.subList(3, 5)).containsExactly("3", "4");
    assertThat(result.sortValues.subList(0, 3))
        .containsExactly(new BsonInt64(120), new BsonInt64(40), new BsonInt64(20))
        .inOrder();
  }

  private record Result(List<String> ids, List<BsonValue> sortValues) {}

  @Test
  public void searchAfter_repeatedParentRead_preservesReduction() throws IOException {
    Sort sort = scopedSort(UserFieldSortOptions.DEFAULT_ASC);
    TermQuery query = new TermQuery(new Term("docType", "root"));
    TopDocs firstPage = this.searcher.search(query, 3, sort);
    FieldDoc after = (FieldDoc) firstPage.scoreDocs[2];

    Result result = readResults(this.searcher.searchAfter(after, query, 2, sort));

    assertThat(after.fields[0]).isEqualTo(new BsonInt64(20));
    assertThat(result.ids).containsExactly("0", "2").inOrder();
    assertThat(result.sortValues).containsExactly(new BsonInt64(30), new BsonInt64(120)).inOrder();
  }

  @Test
  public void topK_competitiveParentWithMultipleChildren_preservesReduction() throws IOException {
    try (IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
      writer.addDocuments(block(5, List.of(child(200, true), child(300, true))));
      writer.forceMerge(1);
    }
    this.searcher.getIndexReader().close();
    this.searcher = new IndexSearcher(DirectoryReader.open(this.directory));

    Result result =
        readResults(
            this.searcher.search(
                new TermQuery(new Term("docType", "root")),
                1,
                scopedSort(UserFieldSortOptions.DEFAULT_DESC)));

    assertThat(result.ids).containsExactly("5");
    assertThat(result.sortValues).containsExactly(new BsonInt64(300));
  }

  @Test
  public void searchAfter_repeatedMissingParentRead_preservesMissingAndNextValue()
      throws IOException {
    try (IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
      writer.addDocuments(block(5, List.of(childNoValue(true), childNoValue(true))));
      writer.addDocuments(block(6, List.of(child(10, true))));
      writer.forceMerge(1);
    }
    this.searcher.getIndexReader().close();
    this.searcher = new IndexSearcher(DirectoryReader.open(this.directory));
    Sort sort = scopedSort(UserFieldSortOptions.DEFAULT_ASC);
    TermQuery query = new TermQuery(new Term("docType", "root"));
    TopDocs firstPage = this.searcher.search(query, 1, sort);
    FieldDoc after = (FieldDoc) firstPage.scoreDocs[0];

    Result result = readResults(this.searcher.searchAfter(after, query, 100, sort));

    assertThat(after.fields[0]).isEqualTo(BsonUtils.MIN_KEY);
    assertThat(result.ids).containsExactly("4", "5", "6", "1", "0", "2").inOrder();
    assertThat(result.sortValues)
        .containsExactly(
            BsonUtils.MIN_KEY,
            BsonUtils.MIN_KEY,
            new BsonInt64(10),
            new BsonInt64(20),
            new BsonInt64(30),
            new BsonInt64(120))
        .inOrder();
  }
}
