package com.xgen.mongot.index.lucene.directory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.LinkedHashMultimap;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.PreloadHint;
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
  private static final Set<String> MMAP_EXTENSIONS =
      Set.of(
          /* Bloom Filter */
          "blm",
          /* Term Index */
          "tip",
          /* Term Dictionary */
          "tim",
          /* Frequencies */
          "doc",
          /* Compound Files */
          "cfs",
          /* Norms */
          "nvd",
          /* DocValues */
          "dvd",
          /* Flat vector data file */
          "vec",
          /* Vector Index */
          "vex",
          /* Quantized flat vector data file */
          "veq",
          /* Field Data: The stored fields for the documents. That would be document ids and
           * stored source in our case, which are looked up when preparing search results. */
          "fdt");

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
  // resolver installed every read gets MADV_NORMAL. Restore context-aware advice, keeping RANDOM
  // (not NORMAL) as the floor to match Lucene 10.1 and avoid read-ahead page-cache thrash on the
  // co-located mongod:
  //  - MERGE/FLUSH stream segments front-to-back -> SEQUENTIAL (read-ahead, few large reads).
  //  - Honor explicit codec hints: DataAccessHint.RANDOM -> RANDOM, DataAccessHint.SEQUENTIAL ->
  // SEQUENTIAL (e.g. flat-vector bulk reads).
  //  - Everything else (term dict/index, postings, norms, docvalues) is random-access search
  // traffic -> RANDOM (the pre-10.4 default). NOTE: NORMAL here would coalesce reads into
  // fewer IOPS and mask the 10.4 query read-amplification on IOPS-bound disks, but risks
  // page-cache thrash; the amplification is better addressed at the scorer, so we keep RANDOM.
  private static Optional<ReadAdvice> adviseByContextDefaultRandom(
      String filename, IOContext context) {
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
    return Optional.of(ReadAdvice.RANDOM);
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
    this.mmapDirectory.setReadAdvice(FileSystemDirectory::adviseByContextDefaultRandom);
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
  }
}
