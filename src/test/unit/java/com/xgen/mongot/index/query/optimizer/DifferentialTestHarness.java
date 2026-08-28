package com.xgen.mongot.index.query.optimizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * Reusable property-based testing utility that lets a rule's test assert semantic equivalence
 * between {@code optimize(q)} and {@code q} against a real (in-memory) Lucene index.
 *
 * <p>Usage: build a harness with a small set of documents, translate both the original and the
 * optimized query trees into Lucene queries, then call {@link #compare} to verify they match the
 * exact same set of documents. The set difference is reported by stable string key (whatever the
 * caller assigned per document) so failures point at concrete diverging documents rather than
 * opaque doc-ids.
 *
 * <p>Indexed field types mirror what vector-index ingestion writes for filter fields, so queries
 * produced by the real translation pipeline execute correctly when the caller supplies mongot's
 * Lucene field names (see {@code FieldName.TypeField}): {@link DocField.StringValue} is a {@link
 * StringField} plus a {@link SortedSetDocValuesField} (the two legs of a token-field equality
 * query); {@link DocField.LongValue} is a {@link LongPoint} (the encoding of int64 numeric filter
 * values). Multi-valued is supported by adding multiple {@link DocField} entries with the same
 * field name on a single {@link Builder#addDocument} call.
 */
public final class DifferentialTestHarness implements AutoCloseable {

  /** Document-id field used to thread a stable key through Lucene's internal docIds. */
  public static final String KEY_FIELD = "__diff_key__";

  private final Directory directory;
  private final DirectoryReader reader;
  private final IndexSearcher searcher;

  private DifferentialTestHarness(Directory directory, DirectoryReader reader) {
    this.directory = directory;
    this.reader = reader;
    this.searcher = new IndexSearcher(reader);
  }

  public static Builder builder() {
    return new Builder();
  }

  public IndexReader reader() {
    return this.reader;
  }

  public IndexSearcher searcher() {
    return this.searcher;
  }

  /**
   * Run both queries and compare matching document sets by their stable {@link #KEY_FIELD} keys.
   * Returns a structured result so the caller can format its own assertion message.
   */
  public Result compare(Query original, Query rewritten) throws IOException {
    Set<String> matchedByOriginal = matchKeys(original);
    Set<String> matchedByRewritten = matchKeys(rewritten);
    if (matchedByOriginal.equals(matchedByRewritten)) {
      return Result.equivalent(matchedByOriginal);
    }
    Set<String> onlyInOriginal = new TreeSet<>(matchedByOriginal);
    onlyInOriginal.removeAll(matchedByRewritten);
    Set<String> onlyInRewritten = new TreeSet<>(matchedByRewritten);
    onlyInRewritten.removeAll(matchedByOriginal);
    return new Result(
        false, matchedByOriginal, matchedByRewritten, onlyInOriginal, onlyInRewritten);
  }

  private Set<String> matchKeys(Query query) throws IOException {
    Set<String> keys = new LinkedHashSet<>();
    var topDocs = this.searcher.search(query, this.reader.maxDoc());
    for (var scoreDoc : topDocs.scoreDocs) {
      Document doc = this.searcher.storedFields().document(scoreDoc.doc);
      String key = doc.get(KEY_FIELD);
      if (key != null) {
        keys.add(key);
      }
    }
    return keys;
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
    this.directory.close();
  }

  /** Outcome of a {@link #compare} call. */
  public record Result(
      boolean equivalent,
      Set<String> matchedByOriginal,
      Set<String> matchedByRewritten,
      Set<String> onlyInOriginal,
      Set<String> onlyInRewritten) {

    static Result equivalent(Set<String> matched) {
      return new Result(true, matched, matched, Set.of(), Set.of());
    }

    /** Concise summary suitable for use as an assertion failure message. */
    public String describe() {
      if (this.equivalent) {
        return "equivalent (" + this.matchedByOriginal.size() + " matching docs)";
      }
      return String.format(
          "INEQUIVALENT: only-in-original=%s only-in-rewritten=%s",
          this.onlyInOriginal, this.onlyInRewritten);
    }
  }

  /** Builds a small in-memory Lucene index for a single test. */
  public static final class Builder {
    private final List<DocumentSpec> specs = new ArrayList<>();

    private Builder() {}

    /**
     * Add a document with the given key and additional indexed fields. Fields with identical names
     * on a single {@code addDocument} call are indexed as multi-valued (Lucene's natural handling
     * of repeated fields on one document).
     */
    public Builder addDocument(String key, Collection<DocField> fields) {
      this.specs.add(new DocumentSpec(key, List.copyOf(fields)));
      return this;
    }

    public Builder addDocument(String key, DocField... fields) {
      return addDocument(key, List.of(fields));
    }

    public DifferentialTestHarness build() throws IOException {
      Directory directory = new ByteBuffersDirectory();
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        for (DocumentSpec spec : this.specs) {
          Document doc = new Document();
          doc.add(new StringField(KEY_FIELD, spec.key, Store.YES));
          for (DocField field : spec.fields) {
            indexField(doc, field);
          }
          writer.addDocument(doc);
        }
      }
      DirectoryReader reader = DirectoryReader.open(directory);
      return new DifferentialTestHarness(directory, reader);
    }

    private static void indexField(Document doc, DocField field) {
      switch (field) {
        case DocField.StringValue sv -> {
          BytesRef value = new BytesRef(sv.value());
          doc.add(new StringField(sv.name(), value, Store.NO));
          doc.add(new SortedSetDocValuesField(sv.name(), value));
        }
        case DocField.LongValue lv -> doc.add(new LongPoint(lv.name(), lv.value()));
      }
    }
  }

  /**
   * Indexable test field. Use {@link #of(String, String)} for string values and {@link
   * #longValue(String, long)} for long-encoded values (int64 numerics, epoch-millis dates).
   * Multi-valued fields are expressed by adding multiple {@code DocField} entries on the same
   * {@link Builder#addDocument} call sharing a name.
   */
  public sealed interface DocField {
    String name();

    /**
     * String-valued field, indexed as a {@link StringField} plus a {@link SortedSetDocValuesField}
     * — the same pair token-field ingestion writes, so both legs of an {@code
     * IndexOrDocValuesQuery} equality see the value.
     */
    static DocField of(String name, String value) {
      return new StringValue(name, value);
    }

    /** Long-valued field indexed as a {@link LongPoint} (point/range matchable). */
    static DocField longValue(String name, long value) {
      return new LongValue(name, value);
    }

    record StringValue(String name, String value) implements DocField {}

    record LongValue(String name, long value) implements DocField {}
  }

  private record DocumentSpec(String key, List<DocField> fields) {}
}
