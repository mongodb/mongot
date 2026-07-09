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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

/**
 * Reusable property-based testing utility that lets a rule's test assert semantic equivalence
 * between {@code optimize(q)} and {@code q} against a real (in-memory) Lucene index.
 *
 * <p>Usage: build a harness with a small set of documents, translate both the original and the
 * optimized query trees into Lucene queries, then call {@link #compare} to verify they match the
 * exact same set of documents. The set difference is reported by stable string key (whatever the
 * caller assigned per document) so failures point at concrete diverging documents rather than
 * opaque doc-ids.
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
     * Add a document with the given key and additional indexed fields. Each value is stored as a
     * {@link StringField} so it can be matched via term/range queries from the test side.
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
            doc.add(new StringField(field.name(), field.value(), Store.NO));
          }
          writer.addDocument(doc);
        }
      }
      DirectoryReader reader = DirectoryReader.open(directory);
      return new DifferentialTestHarness(directory, reader);
    }
  }

  /** A single (name, value) pair to be indexed as a {@link StringField}. */
  public record DocField(String name, String value) {
    public static DocField of(String name, String value) {
      return new DocField(name, value);
    }
  }

  private record DocumentSpec(String key, List<DocField> fields) {}
}
