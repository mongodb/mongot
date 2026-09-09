package com.xgen.mongot.index.lucene.directory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.PreloadHint;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.store.ReadAdvice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We place the majority of index files to {@link NIOFSDirectory}, but the most
 * performance-sensitive data structures reside in {@link MMapDirectory}. Note that we avoid using
 * MMAP for everything due to a risk of affecting co-located mongod which heavily relies on the FS
 * cache. In particular, there is a concern about cache thrashing due to sequential read-ahead when
 * MMAP is used for files with random access pattern (stored fields), see additional details in
 * https://tinyurl.com/2s3ku73k and https://tinyurl.com/mtaa8asy. The second reason to not MMAP 20+
 * other file types is the limitation in max virtual memory areas (vm.max_map_count). mongod also
 * MMAPs files (2 per connection), so we might start competing for this limit if customers would
 * have tens of thousands of indexes on a single node. vm.max_map_count varies by the Atlas tier and
 * currently configured as 153852 on M10, 1048578 on M40 and limited by 4194304 on higher tiers.
 *
 * <p>A description of Lucene file format can be found here:
 * https://lucene.apache.org/core/9_2_0/core/org/apache/lucene/codecs/lucene92/package-summary.html
 */
public class FileSystemDirectory extends FileSwitchDirectory {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemDirectory.class);
  private final MMapDirectory mmapDirectory;
  private final Optional<ByteReadCollector> collector;
  // Fallback read advice per mmapped extension, matching the effective advice on Lucene 10.1 for
  // standalone files. Compound-file interiors differ; see the cfs entry.
  // NORMAL = kernel readahead (files read in runs), RANDOM = one page per fault (point lookups).
  private static final ImmutableMap<String, ReadAdvice> ADVICE_BY_EXTENSION =
      ImmutableMap.<String, ReadAdvice>builder()
          /* Bloom Filter: no 10.1 analog. */
          .put("blm", ReadAdvice.RANDOM)
          /* Term Index: 10.1 opened with RANDOM_PRELOAD, which madvises NORMAL. */
          .put("tip", ReadAdvice.NORMAL)
          /* Term Dictionary: 10.1 default. Random block probes; readahead here thrashes. */
          .put("tim", ReadAdvice.RANDOM)
          /* Frequencies: 10.1 opened with explicit NORMAL for forward-only readahead. */
          .put("doc", ReadAdvice.NORMAL)
          /* Compound Files: 10.1 opened the container with explicit NORMAL. Sub-file slices
           * resolve under this name too, so only an explicit DataAccessHint re-advises a slice
           * (.fdt, vectors). Sub-files without one (.tim, .pos, .pay, .blm) inherit NORMAL, where
           * 10.1 madvised each slice with its own advice and gave them RANDOM. Known deviation
           * from 10.1 (matches 9.11). A segment is compound whenever it is at most 10% of the
           * index, so on large indexes this is every segment. Query-side impact unmeasured. */
          .put("cfs", ReadAdvice.NORMAL)
          /* Norms: 10.1 opened with explicit NORMAL. */
          .put("nvd", ReadAdvice.NORMAL)
          /* DocValues: 10.1 opened with explicit NORMAL. */
          .put("dvd", ReadAdvice.NORMAL)
          /* Flat vector data file */
          .put("vec", ReadAdvice.RANDOM)
          /* Vector Index */
          .put("vex", ReadAdvice.RANDOM)
          /* Quantized flat vector data file */
          .put("veq", ReadAdvice.RANDOM)
          /* Field Data: The stored fields for the documents. That would be document ids and
           * stored source in our case, which are looked up when preparing search results. */
          .put("fdt", ReadAdvice.RANDOM)
          .buildOrThrow();

  // Derived from the advice table so that mmapping a new extension forces an explicit
  // read-advice decision.
  @VisibleForTesting
  static final ImmutableSet<String> MMAP_EXTENSIONS = ADVICE_BY_EXTENSION.keySet();

  // Cache warmer: The filename extensions that we support preloading for.
  // The .vex files are top-priority, .veq files are 2nd-priority, and so on.
  // Note that the .vec files have special handling in the prewarmVectorFiles() function.
  private static final LinkedHashSet<String> prioritizedExtensions =
      new LinkedHashSet<>(
          List.of(
              /* Vector Index */
              "vex",
              /* Quantized flat vector data file */
              "veq",
              /* Flat vector data file */
              "vec"));

  // Cache warmer: Precomputed load order for file extensions.
  private static final LinkedHashSet<String> extensionLoadOrder =
      new LinkedHashSet<>(FileSystemDirectory.prioritizedExtensions.reversed());

  // Lucene 10.4 (apache/lucene #14482) moved read-advice off IOContext into an opt-in
  // per-Directory resolver, and (#15040) flipped the default from RANDOM to NORMAL; with no
  // resolver installed every read gets MADV_NORMAL. Resolution order:
  //  - MERGE/FLUSH stream segments front-to-back -> SEQUENTIAL (read-ahead, few large reads).
  //  - Explicit codec DataAccessHints are honored. Compound-file sub-files resolve under the
  // parent .cfs name, so their DataAccessHint is the only thing that can re-advise a slice.
  //  - Otherwise ADVICE_BY_EXTENSION, with RANDOM as the floor.
  // FileTypeHint/FileDataHint are deliberately not consulted: upstream treats them as semantic
  // labels and places them differently across codec generations (the 10.4 native-codec terms
  // dictionary carries the same bare DATA hint as doc values), so advice derived from them
  // silently changes across Lucene upgrades (CLOUDP-441375, CLOUDP-443288, CLOUDP-443602).
  private static Optional<ReadAdvice> resolveReadAdvice(String filename, IOContext context) {
    if (context.context() == IOContext.Context.MERGE
        || context.context() == IOContext.Context.FLUSH) {
      return Optional.of(ReadAdvice.SEQUENTIAL);
    }
    if (context.hints().contains(DataAccessHint.RANDOM)) {
      return Optional.of(ReadAdvice.RANDOM);
    }
    if (context.hints().contains(DataAccessHint.SEQUENTIAL)) {
      return Optional.of(ReadAdvice.SEQUENTIAL);
    }
    return Optional.of(
        Objects.requireNonNull(
            ADVICE_BY_EXTENSION.getOrDefault(
                FilenameUtils.getExtension(filename), ReadAdvice.RANDOM)));
  }

  public FileSystemDirectory(Path path, Optional<ByteReadCollector> collector) throws IOException {
    this(new MMapDirectory(path), new NIOFSDirectory(path), collector);
  }

  /** Constructs a directory over the given mmap and niofs directories, installing read advice. */
  public FileSystemDirectory(
      MMapDirectory mmapDirectory,
      NIOFSDirectory niofsDirectory,
      Optional<ByteReadCollector> collector) {
    super(MMAP_EXTENSIONS, mmapDirectory, niofsDirectory, true);
    this.mmapDirectory = mmapDirectory;
    this.collector = collector;
    this.mmapDirectory.setReadAdvice(FileSystemDirectory::resolveReadAdvice);
  }

  // OpenInputBase is used for unit test spying.
  @VisibleForTesting
  IndexInput openInputBase(String name, IOContext context) throws IOException {
    return super.openInput(name, context);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    IndexInput delegate = openInputBase(name, context);
    return this.collector
        .<IndexInput>map(c -> new InstrumentedIndexInput(name, c, delegate))
        .orElse(delegate);
  }

  /**
   * Calling prewarmVectorFiles() tells Lucene to load .vex, .veq, and .vec files into the page
   * cache. Exception: A .vec file will only be loaded if there is no matching .veq file. Note,
   * Lucene's setPreload() will always be set to MMapDirectory.NO_FILES by calling
   * prewarmVectorFiles().
   */
  public void prewarmVectorFiles() {
    try {
      LOG.atInfo()
          .addKeyValue("directory", this.mmapDirectory.getDirectory())
          .log("Cache Warmer: warming directory");

      this.mmapDirectory.setPreload(MMapDirectory.PRELOAD_HINT);

      // Scan all the base filenames that have filename extensions we're interested in warming.
      LinkedHashMultimap<String, String> extensionsAndBaseNames = LinkedHashMultimap.create();
      String[] files = this.mmapDirectory.listAll();
      for (String fileName : files) {
        String ex = FilenameUtils.getExtension(fileName);
        if (FileSystemDirectory.extensionLoadOrder.contains(ex)) {
          String bn = FilenameUtils.getBaseName(fileName);
          extensionsAndBaseNames.put(ex, bn);
        }
      }

      // Don't preload vector data files if we have quantized data for the same segment.
      extensionsAndBaseNames.get("vec").removeAll(extensionsAndBaseNames.get("veq"));

      // Load the selected set of vector files into the page cache.
      // The original ordering from MMapDirectory.listAll() is preserved for aesthetic reasons
      // except the extensions are warmed in reverse prioritizedExtensions order.
      // Meaning that all .vec or .veq files will be warmed before all .vex files.
      for (String ex : FileSystemDirectory.extensionLoadOrder) {
        for (String bn : extensionsAndBaseNames.get(ex)) {
          String fileName = bn + "." + ex;
          LOG.atDebug().addKeyValue("file", fileName).log("Cache Warmer: warming segment");
          try (IndexInput input =
              this.mmapDirectory.openInput(
                  fileName, IOContext.DEFAULT.withHints(PreloadHint.INSTANCE))) {
            // The openInput() call with PreloadHint plus the above setPreload(PRELOAD_HINT) call
            // is expected to trigger the preload.
          }
        }
      }
      // Note that if files are being evicted from the page cache during warming then by definition
      // the customer's cluster is under-provisioned. However we make a little effort to help the
      // under-provisioned case by warming the most important extensions last so that pages of
      // lesser importance will be evicted. This effort (prioritizedExtensions ordering) would be
      // more effective if done as a global, multi-Directory startup step rather than per-Directory,
      // but this is a good start.
    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("directory", this.mmapDirectory.getDirectory())
          .setCause(e)
          .log("Cache Warmer: failure (ignored because this is only an optimization)");
    } finally {
      this.mmapDirectory.setPreload(MMapDirectory.NO_FILES); // No way to discover the prior value.
    }
  }

  static class InstrumentedIndexInput extends IndexInput {
    private final ByteReadCollector collector;
    private final String name;
    private final IndexInput delegate;
    private final String extension;

    public InstrumentedIndexInput(String name, ByteReadCollector collector, IndexInput in) {
      super(name);
      this.name = name;
      this.delegate = in;
      this.extension = getExtension(name);
      this.collector = collector;
    }

    @Override
    public void close() throws IOException {
      this.delegate.close();
    }

    @Override
    public long getFilePointer() {
      return this.delegate.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      this.delegate.seek(pos);
    }

    @Override
    public long length() {
      return this.delegate.length();
    }

    @Override
    public IndexInput clone() {
      return new InstrumentedIndexInput(this.name, this.collector, this.delegate.clone());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new InstrumentedIndexInput(
          this.name, this.collector, this.delegate.slice(sliceDescription, offset, length));
    }

    @Override
    public byte readByte() throws IOException {
      this.collector.collect(this.extension, 1);
      return this.delegate.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      this.collector.collect(this.extension, len);
      this.delegate.readBytes(b, offset, len);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
      this.collector.collect(this.extension, len);
      this.delegate.readBytes(b, offset, len, useBuffer);
    }

    /**
     * Delegates to the wrapped input's random-access slice instead of inheriting {@link
     * IndexInput}'s fallback.
     *
     * <p>That fallback implements positional reads as {@code seek} followed by a relative read on a
     * single mutable slice, so it is not safe for concurrent use. Callers may share one slice
     * across threads — the {@code _id} bloom filter does exactly that, handing one slice per field
     * to a {@code MongotFuzzySet} that every query thread probes (see {@code
     * MongotFuzzySet#fromInput}) — and concurrent positional reads through the fallback race the
     * file pointer and return the wrong bytes. Reading the wrong bloom word yields a false
     * negative, which suppresses a document that should have matched.
     *
     * <p>The wrapped input's own implementation is positional and stateless (for {@code
     * MMapDirectory} it reads straight from the mapped segment), so delegating keeps both the
     * thread safety and the byte accounting.
     */
    @Override
    public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
      return new InstrumentedRandomAccessInput(
          this.extension, this.collector, this.delegate.randomAccessSlice(offset, length));
    }
  }

  /**
   * Counts bytes read through a {@link RandomAccessInput} without adding mutable state, so it stays
   * as safe for concurrent positional reads as the input it wraps.
   */
  static class InstrumentedRandomAccessInput implements RandomAccessInput {
    private final String extension;
    private final ByteReadCollector collector;
    private final RandomAccessInput delegate;

    InstrumentedRandomAccessInput(
        String extension, ByteReadCollector collector, RandomAccessInput delegate) {
      this.extension = extension;
      this.collector = collector;
      this.delegate = delegate;
    }

    @Override
    public long length() {
      return this.delegate.length();
    }

    @Override
    public byte readByte(long pos) throws IOException {
      this.collector.collect(this.extension, 1);
      return this.delegate.readByte(pos);
    }

    @Override
    public void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
      this.collector.collect(this.extension, length);
      this.delegate.readBytes(pos, bytes, offset, length);
    }

    @Override
    public short readShort(long pos) throws IOException {
      this.collector.collect(this.extension, Short.BYTES);
      return this.delegate.readShort(pos);
    }

    @Override
    public int readInt(long pos) throws IOException {
      this.collector.collect(this.extension, Integer.BYTES);
      return this.delegate.readInt(pos);
    }

    @Override
    public long readLong(long pos) throws IOException {
      this.collector.collect(this.extension, Long.BYTES);
      return this.delegate.readLong(pos);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
      this.delegate.prefetch(offset, length);
    }

    @Override
    public java.util.Optional<Boolean> isLoaded() {
      return this.delegate.isLoaded();
    }
  }
}
