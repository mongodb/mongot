package com.xgen.testing.util;

import com.google.common.primitives.Ints;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.ForceMergePolicy;
import org.apache.lucene.tests.util.TestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link IndexWriter} creates a random number of segments for each run. Creating segments is
 * slow, so this writer should likely not be used broadly in unit tests if the class under test does
 * not implement logic to merge info or do other work across segments.
 *
 * <p>The recommended usage of this class is to use the non-deterministic constructor {@link
 * #RandomSegmentingIndexWriter(Directory)} in unit tests, and use the deterministic constructor
 * {@link #RandomSegmentingIndexWriter(Directory, IndexWriterConfig, Random)} to debug failures.
 */
public class RandomSegmentingIndexWriter extends IndexWriter implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RandomSegmentingIndexWriter.class);

  private final Random rng;

  private static Random createNondeterministicRandom() {
    long seed = ThreadLocalRandom.current().nextLong();
    LOG.info("Creating RandomSegmentingIndexWriter with seed={}", seed);
    return new Random(seed);
  }

  /** Creates a non-deterministic writer that will produce different segments for each run. */
  @MustBeClosed
  public RandomSegmentingIndexWriter(Directory d) throws IOException {
    this(d, new IndexWriterConfig());
  }

  /**
   * Creates a non-deterministic writer that will produce different segments for each run.
   *
   * <p>Convenience method for {@code new RandomMergingIndexWriter(d, conf, new Random())}
   */
  @MustBeClosed
  public RandomSegmentingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
    this(d, conf, createNondeterministicRandom());
  }

  /**
   * Constructs a new IndexWriter per the settings given in <code>conf</code> and a Random instance
   * for reproducible runs.
   *
   * <p><b>NOTE:</b> The merge policy and commitOnClose setting of the config will be overwritten by
   * this class. After ths writer is created, the given configuration instance cannot be passed to
   * another writer.
   *
   * @param d the index directory. The index is either created or appended according <code>
   *             conf.getOpenMode()</code>.
   * @param conf the configuration settings according to which IndexWriter should be initialized.
   * @param rng the random number generator
   * @throws IOException if the directory cannot be read/written to, or if it does not exist and
   *     <code>conf.getOpenMode()</code> is <code>OpenMode.APPEND</code> or if there is any other
   *     low-level IO error
   */
  @MustBeClosed
  public RandomSegmentingIndexWriter(Directory d, IndexWriterConfig conf, Random rng)
      throws IOException {
    super(
        d,
        conf.setMergePolicy(new ForceMergePolicy(conf.getMergePolicy()))
            .setCommitOnClose(true)
            // Set to 0 to disable merging on full flush
            .setMaxFullFlushMergeWaitMillis(0));
    this.rng = rng;
  }

  @Override
  public long updateDocument(Term term, Iterable<? extends IndexableField> doc) throws IOException {
    var result = super.updateDocument(term, doc);
    if (this.rng.nextDouble() <= (1.0 / super.getPendingNumDocs())) {
      // Commits are slow, so this sampling aims to grow segment count with ~ln(num_docs)
      super.commit();
    }

    return result;
  }

  /**
   * Adds a document to the index, randomly committing along the way.
   *
   * @deprecated This method only creates a random number of segments at increasingly long
   *     intervals. It does not reorder documents or guarantee that each documents has equal
   *     probability of appearing in each segment, nor does it reorder docIDs. For much more
   *     thorough randomization, prefer bulk adding documents with {@link #addDocuments(Iterable)}
   */
  @Override
  @Deprecated
  public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
    return super.addDocument(doc);
  }

  @Override
  public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs)
      throws IOException {
    // Step 1: Randomize order of documents
    List<Iterable<? extends IndexableField>> list = new ArrayList<>();
    docs.forEach(list::add);
    Collections.shuffle(list, this.rng);

    if (this.rng.nextInt(100) == 0 || list.size() <= 1) {
      // 1% chance of generating a single segment, which is otherwise not possible below.
      return super.addDocuments(list);
    }

    // Step 2: Define up to 9 monotonic indices to insert commits at, with implicit 10th at end
    int maxBreaks = Ints.constrainToRange(9, 0, list.size());
    int[] breakPoints =
        IntStream.generate(() -> TestUtil.nextInt(this.rng, 1, list.size()))
            .limit(maxBreaks)
            .sorted()
            .distinct()
            .toArray();
    breakPoints[0] = 1; // Let's guarantee at least one segment of size=1

    // Step 3: Commit after each slice. Some slices may be empty, but that just increases randomness
    @Var int start = 0;
    for (int breakPoint : breakPoints) {
      super.addDocuments(list.subList(start, breakPoint));
      super.commit();
      start = breakPoint;
    }
    return super.addDocuments(list.subList(start, list.size()));
  }
}
