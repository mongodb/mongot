/*
 * Derived from lucene-mongot 10.1.0 BloomFilteringPostingsFormat.
 */

package com.xgen.mongot.index.lucene.codec.bloom;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.codec.bloom.MongotFuzzySet.ContainsResult;
import com.xgen.mongot.util.GlobalMetricFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link PostingsFormat} useful for low doc-frequency fields such as primary keys. Bloom filters
 * are maintained in a ".blm" file which offers "fast-fail" for reads in segments known to have no
 * record of the key. A choice of delegate PostingsFormat is used to record all other Postings data.
 *
 * <p><b>Upstream source.</b> Copied from lucene-mongot 10.1.0 {@code
 * org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat} in the {@code lucene-codecs} module
 * (see {@code
 * lucene/codecs/src/java/org/apache/lucene/codecs/bloom/BloomFilteringPostingsFormat.java} in the
 * Lucene 10.1.0 release tree). On-disk layout matches upstream except for the codec name written in
 * the {@code .blm} header (see below).
 *
 * <p><b>Write path.</b> Indexing does not pick this class via ServiceLoader. {@link
 * com.xgen.mongot.index.lucene.codec.HybridPostingsFormat}, installed on {@link
 * com.xgen.mongot.index.lucene.codec.LuceneCodec}, constructs {@code new
 * MongotBloomFilteringPostingsFormat(delegate)} for the {@code _id} field when a {@link
 * java.util.function.BooleanSupplier} returns {@code true}. {@link
 * com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter} builds that supplier from dynamic
 * feature flags ({@link
 * com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags#BLOOM_FILTER_FOR_ID_FIELD}), whether
 * natural-order scan is enabled, and index type (auto-embedding indices disable bloom). The
 * supplier is consulted again on flush and merge, so adjacent segments may use different encodings.
 * The parameterless constructor is for Lucene read-time SPI only and must not be used when opening
 * an {@link org.apache.lucene.index.IndexWriter}.
 *
 * <p><b>Read path.</b> Lucene loads this class from the {@code .blm} index header when the segment
 * was written with {@link #BLOOM_CODEC_NAME}. Whether bloom fast-fail is wired up on read is
 * controlled separately by {@link MongotBloomReadPolicy} (also driven by the same feature-flag
 * policy as the write supplier). When enabled, the fuzzy-set bitsets are <em>not</em> copied onto
 * the heap: each field's set is backed by a {@link org.apache.lucene.store.RandomAccessInput} slice
 * over the memory-mapped {@code .blm} (see {@link MongotFuzzySet#fromInput} and {@link
 * com.xgen.mongot.index.lucene.directory.FileSystemDirectory}, which maps the {@code .blm}
 * extension), so the bits stay in the OS page cache.
 *
 * <p><b>Differences from upstream.</b>
 *
 * <ul>
 *   <li><b>SPI / codec name</b> — {@link #BLOOM_CODEC_NAME} is {@code MongotBloomFilter} instead of
 *       upstream {@code BloomFilter}, so this class is registered separately from Lucene's built-in
 *       format and segments written here store the mongot name in the index header.
 *   <li><b>Conditional bloom load on read</b> — {@link
 *       MongotBloomReadPolicy#shouldLoadBloomOnRead()} controls whether the input-backed {@link
 *       MongotFuzzySet} fast-fail is wired up. When false, {@link #fieldsProducer} returns the
 *       delegate {@link FieldsProducer} directly: only the delegate name is read from the {@code
 *       .blm} header (no bloom sets opened); term lookups use delegate postings only (no
 *       fast-fail).
 *   <li><b>Bloom eviction and close</b> — {@link MongotBloomFilteredFieldsProducer#close()} clears
 *       {@code bloomsByFieldName} and then closes the {@code .blm} input, so bloom state is dropped
 *       when the segment reader is closed.
 * </ul>
 *
 * <p>A choice of {@link MongotBloomFilterFactory} can be passed to tailor Bloom Filter settings on
 * a per-field basis. The default configuration allocates a ~8mb bitset and hashes values using
 * MurmurHash64. This should be suitable for most purposes.
 *
 * <p>The format of the blm file is as follows:
 *
 * <ul>
 *   <li>BloomFilter (.blm) --&gt; Header, DelegatePostingsFormatName, NumFilteredFields,
 *       Filter<sup>NumFilteredFields</sup>, Footer
 *   <li>Filter --&gt; FieldNumber, MongotFuzzySet
 *   <li>MongotFuzzySet --&gt;See {@link MongotFuzzySet#serialize(DataOutput)}
 *   <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *   <li>DelegatePostingsFormatName --&gt; {@link DataOutput#writeString(String) String} The name of
 *       a ServiceProvider registered {@link PostingsFormat}
 *   <li>NumFilteredFields --&gt; {@link DataOutput#writeInt Uint32}
 *   <li>FieldNumber --&gt; {@link DataOutput#writeInt Uint32} The number of the field in this
 *       segment
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * @lucene.experimental
 */
@AutoService(PostingsFormat.class)
public final class MongotBloomFilteringPostingsFormat extends PostingsFormat {

  public static final String BLOOM_CODEC_NAME = "MongotBloomFilter";
  public static final int VERSION_START = 3;
  public static final int VERSION_CURRENT = VERSION_START;

  /** Extension of Bloom Filters file */
  static final String BLOOM_EXTENSION = "blm";

  private final MongotBloomFilterFactory bloomFilterFactory;
  private final PostingsFormat delegatePostingsFormat;

  /**
   * Creates Bloom filters for a selection of fields created in the index. This is recorded as a set
   * of Bitsets held as a segment summary in an additional "blm" file. This PostingsFormat delegates
   * to a choice of delegate PostingsFormat for encoding all other postings data.
   *
   * <p>Read/write bloom counters are published process-wide via {@link GlobalMetricFactory}, so no
   * metrics context needs to be threaded through this format or its Service Provider read-path
   * instances.
   *
   * @param delegatePostingsFormat The PostingsFormat that records all the non-bloom filter data
   *     i.e. postings info.
   * @param bloomFilterFactory The {@link MongotBloomFilterFactory} responsible for sizing
   *     BloomFilters appropriately
   */
  public MongotBloomFilteringPostingsFormat(
      PostingsFormat delegatePostingsFormat, MongotBloomFilterFactory bloomFilterFactory) {
    super(BLOOM_CODEC_NAME);
    this.delegatePostingsFormat = delegatePostingsFormat;
    this.bloomFilterFactory = bloomFilterFactory;
  }

  /**
   * Creates Bloom filters for a selection of fields created in the index. This is recorded as a set
   * of Bitsets held as a segment summary in an additional "blm" file. This PostingsFormat delegates
   * to a choice of delegate PostingsFormat for encoding all other postings data. This choice of
   * constructor defaults to the {@link MongotBloomFilterFactory} for configuring per-field
   * BloomFilters.
   *
   * @param delegatePostingsFormat The PostingsFormat that records all the non-bloom filter data
   *     i.e. postings info.
   */
  public MongotBloomFilteringPostingsFormat(PostingsFormat delegatePostingsFormat) {
    this(delegatePostingsFormat, new MongotBloomFilterFactory());
  }

  // Used only at read-time via Service Provider instantiation -
  // do not use at Write-time in application code.
  public MongotBloomFilteringPostingsFormat() {
    this(null, new MongotBloomFilterFactory());
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    if (delegatePostingsFormat == null) {
      throw new UnsupportedOperationException(
          "Error - "
              + getClass().getName()
              + " has been constructed without a choice of PostingsFormat");
    }
    FieldsConsumer fieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);
    return new MongotBloomFilteredFieldsConsumer(fieldsConsumer, state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    if (!MongotBloomReadPolicy.shouldLoadBloomOnRead(state.segmentInfo.dir)) {
      return resolveDelegatePostingsFormat(state).fieldsProducer(state);
    }
    return new MongotBloomFilteredFieldsProducer(state);
  }

  private PostingsFormat resolveDelegatePostingsFormat(SegmentReadState state) throws IOException {
    if (this.delegatePostingsFormat != null) {
      return this.delegatePostingsFormat;
    }
    return PostingsFormat.forName(readDelegatePostingsFormatNameFromBlm(state));
  }

  /**
   * Reads only the delegate {@link PostingsFormat} name from the segment {@code .blm} sidecar
   * (index header + string). Does not load bloom bitsets.
   */
  private static String readDelegatePostingsFormatNameFromBlm(SegmentReadState state)
      throws IOException {
    try (ChecksumIndexInput bloomIn = openBlmChecksumInput(state)) {
      return bloomIn.readString();
    }
  }

  private static ChecksumIndexInput openBlmChecksumInput(SegmentReadState state)
      throws IOException {
    String bloomFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
    ChecksumIndexInput bloomIn = state.directory.openChecksumInput(bloomFileName);
    CodecUtil.checkIndexHeader(
        bloomIn,
        BLOOM_CODEC_NAME,
        VERSION_START,
        VERSION_CURRENT,
        state.segmentInfo.getId(),
        state.segmentSuffix);
    return bloomIn;
  }

  /**
   * Opens the segment {@code .blm} via the directory and positions past the index header. Unlike
   * {@link #openBlmChecksumInput}, this returns a plain {@link IndexInput} that supports {@link
   * IndexInput#randomAccessSlice} so per-field bitsets can be read straight from the memory-mapped
   * file (see {@link com.xgen.mongot.index.lucene.directory.FileSystemDirectory}, which maps the
   * {@code .blm} extension). The caller owns the returned input and must close it.
   */
  private static IndexInput openBlmInput(SegmentReadState state) throws IOException {
    String bloomFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
    IndexInput bloomIn = state.directory.openInput(bloomFileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkIndexHeader(
          bloomIn,
          BLOOM_CODEC_NAME,
          VERSION_START,
          VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      success = true;
      return bloomIn;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(bloomIn);
      }
    }
  }

  /**
   * Per-field {@code .blm} layout captured while validating the file, used to build read slices.
   */
  private record FieldBloomMeta(
      int fieldNum, int hashCount, int bloomSize, int numWords, long wordsOffset) {}

  static class MongotBloomFilteredFieldsProducer extends FieldsProducer {
    private FieldsProducer delegateFieldsProducer;
    private final Map<String, MongotFuzzySet> bloomsByFieldName = new ConcurrentHashMap<>();
    private final List<MongotBloomFieldsProducerRegistry.Key> registryKeys = new ArrayList<>();

    // The open .blm input, under StorageMode.INPUT only. Each field's MongotFuzzySet
    // reads its bitset words straight from random-access slices of this input, so it must stay open
    // for the life of the producer (the slices become invalid once it is closed). Closed in
    // close(). Null under StorageMode.HEAP, where the words are copied onto the heap at open and
    // the .blm is not held open.
    @Nullable private IndexInput bloomInput;

    public MongotBloomFilteredFieldsProducer(SegmentReadState state) throws IOException {
      if (BloomStoragePolicy.inputReadPathEnabled()) {
        openBloomsFromInput(state);
      } else {
        openBloomsOnHeap(state);
      }
      boolean registered = false;
      try {
        for (String fieldName : bloomsByFieldName.keySet()) {
          registryKeys.add(MongotBloomFieldsProducerRegistry.register(state, fieldName, this));
        }
        registered = true;
      } finally {
        if (!registered) {
          IOUtils.closeWhileHandlingException(delegateFieldsProducer, bloomInput);
        }
      }
    }

    /**
     * Reads the {@code .blm} in two passes and backs each field's set with a random-access slice
     * over the memory-mapped file, so the bitset words stay in the OS page cache rather than being
     * copied onto the JVM heap. Used under {@link MongotFuzzySet.StorageMode#INPUT}.
     */
    private void openBloomsFromInput(SegmentReadState state) throws IOException {
      ChecksumIndexInput checksumIn = null;
      boolean success = false;
      try {
        // Pass 1: validate the .blm (index header + footer checksum) and record where each field's
        // serialized bitset words live. skipBytes() keeps the running checksum correct without
        // copying the words onto the heap.
        checksumIn = openBlmChecksumInput(state);
        String delegateFormatName = checksumIn.readString();
        int numBlooms = checksumIn.readInt();
        List<FieldBloomMeta> metas = new ArrayList<>(numBlooms);
        for (int i = 0; i < numBlooms; i++) {
          int fieldNum = checksumIn.readInt();
          int hashCount = checksumIn.readVInt();
          int bloomSize = checksumIn.readInt();
          int numWords = checksumIn.readInt();
          long wordsOffset = checksumIn.getFilePointer();
          metas.add(new FieldBloomMeta(fieldNum, hashCount, bloomSize, numWords, wordsOffset));
          checksumIn.skipBytes((long) numWords * Long.BYTES);
        }
        CodecUtil.checkFooter(checksumIn);
        IOUtils.close(checksumIn);
        checksumIn = null;

        // Pass 2: open the .blm through the directory and back each field's fuzzy set
        // with a random-access slice over its word region (no heap copy of the bitset).
        this.bloomInput = openBlmInput(state);
        this.delegateFieldsProducer =
            PostingsFormat.forName(delegateFormatName).fieldsProducer(state);
        for (FieldBloomMeta meta : metas) {
          RandomAccessInput words =
              bloomInput.randomAccessSlice(meta.wordsOffset(), (long) meta.numWords() * Long.BYTES);
          MongotFuzzySet bloom =
              MongotFuzzySet.fromInput(words, meta.hashCount(), meta.bloomSize(), meta.numWords());
          FieldInfo fieldInfo = state.fieldInfos.fieldInfo(meta.fieldNum());
          bloomsByFieldName.put(fieldInfo.name, bloom);
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(checksumIn, bloomInput, delegateFieldsProducer);
        }
      }
    }

    /**
     * Reads the {@code .blm} in a single pass, copying each field's bitset words onto the JVM heap
     * and closing the file. Used under {@link MongotFuzzySet.StorageMode#HEAP}, and reads the
     * identical on-disk format.
     */
    private void openBloomsOnHeap(SegmentReadState state) throws IOException {
      ChecksumIndexInput bloomIn = null;
      boolean success = false;
      try {
        bloomIn = openBlmChecksumInput(state);
        this.delegateFieldsProducer =
            PostingsFormat.forName(bloomIn.readString()).fieldsProducer(state);
        int numBlooms = bloomIn.readInt();
        for (int i = 0; i < numBlooms; i++) {
          int fieldNum = bloomIn.readInt();
          MongotFuzzySet bloom = MongotFuzzySet.deserialize(bloomIn);
          FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
          bloomsByFieldName.put(fieldInfo.name, bloom);
        }
        CodecUtil.checkFooter(bloomIn);
        IOUtils.close(bloomIn);
        bloomIn = null;
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(bloomIn, delegateFieldsProducer);
        }
      }
    }

    @Override
    public Iterator<String> iterator() {
      return delegateFieldsProducer.iterator();
    }

    @Override
    public void close() throws IOException {
      for (MongotBloomFieldsProducerRegistry.Key key : registryKeys) {
        MongotBloomFieldsProducerRegistry.unregister(key, this);
      }
      registryKeys.clear();
      bloomsByFieldName.clear();
      // Closing bloomInput unmaps the .blm and invalidates every field's random-access slice, so it
      // must happen only after the bloom sets are dropped from bloomsByFieldName above.
      IOUtils.close(delegateFieldsProducer, bloomInput);
    }

    @Override
    public Terms terms(String field) throws IOException {
      MongotFuzzySet filter = bloomsByFieldName.get(field);
      if (filter == null) {
        return delegateFieldsProducer.terms(field);
      } else {
        Terms result = delegateFieldsProducer.terms(field);
        if (result == null) {
          return null;
        }
        return new MongotBloomFilteredTerms(result, filter);
      }
    }

    @Override
    public int size() {
      return delegateFieldsProducer.size();
    }

    @VisibleForTesting
    MongotFuzzySet getLoadedBloomForField(String fieldName) {
      return bloomsByFieldName.get(fieldName);
    }

    /**
     * True if this segment currently has any bloom sets loaded, i.e. whether the fast-fail
     * structures are wired up. Bloom sets read through the input (see {@link
     * MongotFuzzySet#fromInput}), so this says nothing about heap residency.
     */
    boolean hasLoadedBloom() {
      return !bloomsByFieldName.isEmpty();
    }

    /**
     * JVM heap bytes held by this segment's bloom sets. Non-zero under {@link
     * MongotFuzzySet.StorageMode#HEAP}, where the words are copied onto the heap at open; {@code 0}
     * under {@link MongotFuzzySet.StorageMode#INPUT}, where they live in the OS page cache behind
     * the memory-mapped {@code .blm}.
     */
    long heapBloomRamBytesUsed() {
      @Var long total = 0;
      for (MongotFuzzySet bloom : bloomsByFieldName.values()) {
        total += bloom.ramBytesUsed();
      }
      return total;
    }

    /**
     * Size of this segment's bloom bitsets in the OS page cache behind the memory-mapped {@code
     * .blm}, under {@link MongotFuzzySet.StorageMode#INPUT}; {@code 0} under {@link
     * MongotFuzzySet.StorageMode#HEAP}, where the footprint is on the heap instead. Backs the
     * {@code bloomFilterMappedBytes} gauge.
     */
    long mappedBloomBytes() {
      @Var long total = 0;
      for (MongotFuzzySet bloom : bloomsByFieldName.values()) {
        // bitsetBytes() is the serialized size regardless of backing, so only count sets that are
        // actually mapped; under HEAP those same bytes are reported by heapBloomRamBytesUsed().
        if (bloom.readPathStorageMode() == MongotFuzzySet.StorageMode.INPUT) {
          total += bloom.bitsetBytes();
        }
      }
      return total;
    }

    /**
     * Drops the bloom sets (and thus fast-fail) while keeping the delegate postings reader open.
     * The {@code .blm} input stays open until {@link #close()}; only the per-field {@link
     * MongotFuzzySet} references are released here.
     *
     * <p>Safe to call while other threads search the same segment: {@link #terms} only reads the
     * map, and in-flight {@link MongotBloomFilteredTerms} instances retain their {@link
     * MongotFuzzySet} reference. {@link ConcurrentHashMap} makes {@code clear()} safe with
     * concurrent {@code get}.
     */
    void evictBloom() {
      bloomsByFieldName.clear();
    }

    static class MongotBloomFilteredTerms extends Terms {
      private Terms delegateTerms;
      private MongotFuzzySet filter;

      public MongotBloomFilteredTerms(Terms terms, MongotFuzzySet filter) {
        this.delegateTerms = terms;
        this.filter = filter;
      }

      @Override
      public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm)
          throws IOException {
        return delegateTerms.intersect(compiled, startTerm);
      }

      @Override
      public TermsEnum iterator() throws IOException {
        return new MongotBloomFilteredTermsEnum(delegateTerms, filter);
      }

      @Override
      public long size() throws IOException {
        return delegateTerms.size();
      }

      @Override
      public long getSumTotalTermFreq() throws IOException {
        return delegateTerms.getSumTotalTermFreq();
      }

      @Override
      public long getSumDocFreq() throws IOException {
        return delegateTerms.getSumDocFreq();
      }

      @Override
      public int getDocCount() throws IOException {
        return delegateTerms.getDocCount();
      }

      @Override
      public boolean hasFreqs() {
        return delegateTerms.hasFreqs();
      }

      @Override
      public boolean hasOffsets() {
        return delegateTerms.hasOffsets();
      }

      @Override
      public boolean hasPositions() {
        return delegateTerms.hasPositions();
      }

      @Override
      public boolean hasPayloads() {
        return delegateTerms.hasPayloads();
      }

      @Override
      public BytesRef getMin() throws IOException {
        return delegateTerms.getMin();
      }

      @Override
      public BytesRef getMax() throws IOException {
        return delegateTerms.getMax();
      }
    }

    static final class MongotBloomFilteredTermsEnum extends BaseTermsEnum {
      private Terms delegateTerms;
      private TermsEnum delegateTermsEnum;
      private final MongotFuzzySet filter;

      public MongotBloomFilteredTermsEnum(Terms delegateTerms, MongotFuzzySet filter)
          throws IOException {
        this.delegateTerms = delegateTerms;
        this.filter = filter;
      }

      void reset(Terms delegateTerms) throws IOException {
        this.delegateTerms = delegateTerms;
        this.delegateTermsEnum = null;
      }

      private TermsEnum delegate() throws IOException {
        if (delegateTermsEnum == null) {
          /* pull the iterator only if we really need it -
           * this can be a relativly heavy operation depending on the
           * delegate postings format and they underlying directory
           * (clone IndexInput) */
          delegateTermsEnum = delegateTerms.iterator();
        }
        return delegateTermsEnum;
      }

      @Override
      public BytesRef next() throws IOException {
        return delegate().next();
      }

      @Override
      public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
        // The magical fail-fast speed up that is the entire point of all of
        // this code - save a disk seek if there is a match on an in-memory
        // structure
        // that may occasionally give a false positive but guaranteed no false
        // negatives
        if (filter.contains(text) == ContainsResult.NO) {
          GlobalMetricFactory.incrementBloomFilterIdLookupSkipped();
          return null;
        }
        GlobalMetricFactory.incrementBloomFilterIdLookupProbed();
        IOBooleanSupplier delegateSupplier = delegate().prepareSeekExact(text);
        if (delegateSupplier == null) {
          // Bloom admitted the term but the delegate has no matching term: a bloom false positive.
          // Mirror the seekExact path so false positives are not undercounted here.
          GlobalMetricFactory.incrementBloomFilterIdLookupFalsePositive();
          return null;
        }
        return () -> {
          boolean found = delegateSupplier.get();
          if (!found) {
            GlobalMetricFactory.incrementBloomFilterIdLookupFalsePositive();
          }
          return found;
        };
      }

      @Override
      public boolean seekExact(BytesRef text) throws IOException {
        // See #prepareSeekExact
        if (filter.contains(text) == ContainsResult.NO) {
          GlobalMetricFactory.incrementBloomFilterIdLookupSkipped();
          return false;
        }
        GlobalMetricFactory.incrementBloomFilterIdLookupProbed();
        boolean found = delegate().seekExact(text);
        if (!found) {
          GlobalMetricFactory.incrementBloomFilterIdLookupFalsePositive();
        }
        return found;
      }

      @Override
      public SeekStatus seekCeil(BytesRef text) throws IOException {
        return delegate().seekCeil(text);
      }

      @Override
      public void seekExact(long ord) throws IOException {
        delegate().seekExact(ord);
      }

      @Override
      public BytesRef term() throws IOException {
        return delegate().term();
      }

      @Override
      public long ord() throws IOException {
        return delegate().ord();
      }

      @Override
      public int docFreq() throws IOException {
        return delegate().docFreq();
      }

      @Override
      public long totalTermFreq() throws IOException {
        return delegate().totalTermFreq();
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        return delegate().postings(reuse, flags);
      }

      @Override
      public ImpactsEnum impacts(int flags) throws IOException {
        return delegate().impacts(flags);
      }

      @Override
      public boolean preferSeekExact() {
        // Prefer seekExact() to seekCeil() when processing updates and deletes,
        // since seekExact() passes through the bloom filter.
        return true;
      }

      @Override
      public String toString() {
        return getClass().getSimpleName() + "(filter=" + filter.toString() + ")";
      }
    }

    @Override
    public void checkIntegrity() throws IOException {
      delegateFieldsProducer.checkIntegrity();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName()
          + "(fields="
          + bloomsByFieldName.size()
          + ",delegate="
          + delegateFieldsProducer
          + ")";
    }
  }

  class MongotBloomFilteredFieldsConsumer extends FieldsConsumer {
    private FieldsConsumer delegateFieldsConsumer;
    private Map<FieldInfo, MongotFuzzySet> bloomFilters = new HashMap<>();
    private SegmentWriteState state;

    public MongotBloomFilteredFieldsConsumer(
        FieldsConsumer fieldsConsumer, SegmentWriteState state) {
      this.delegateFieldsConsumer = fieldsConsumer;
      this.state = state;
    }

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {

      // Delegate must write first: it may have opened files
      // on creating the class
      // (e.g. Lucene41PostingsConsumer), and write() will
      // close them; alternatively, if we delayed pulling
      // the fields consumer until here, we could do it
      // afterwards:
      delegateFieldsConsumer.write(fields, norms);

      for (String field : fields) {
        Terms terms = fields.terms(field);
        if (terms == null) {
          continue;
        }
        FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        TermsEnum termsEnum = terms.iterator();

        MongotFuzzySet bloomFilter = null;

        PostingsEnum postingsEnum = null;
        while (true) {
          BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          if (bloomFilter == null) {
            bloomFilter = bloomFilterFactory.getSetForField(state, fieldInfo);
            if (bloomFilter == null) {
              // Field not bloom'd
              break;
            }
            assert bloomFilters.containsKey(fieldInfo) == false;
            bloomFilters.put(fieldInfo, bloomFilter);
          }
          // Make sure there's at least one doc for this term:
          postingsEnum = termsEnum.postings(postingsEnum, 0);
          if (postingsEnum.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
            bloomFilter.addValue(term);
          }
        }
      }
    }

    private boolean closed;

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      delegateFieldsConsumer.close();

      // Now we are done accumulating values for these fields
      List<Entry<FieldInfo, MongotFuzzySet>> nonSaturatedBlooms = new ArrayList<>();

      for (Entry<FieldInfo, MongotFuzzySet> entry : bloomFilters.entrySet()) {
        MongotFuzzySet bloomFilter = entry.getValue();
        if (!bloomFilterFactory.isSaturated(bloomFilter, entry.getKey())) {
          nonSaturatedBlooms.add(entry);
        }
      }
      int saturatedDropped = bloomFilters.size() - nonSaturatedBlooms.size();
      if (saturatedDropped > 0) {
        GlobalMetricFactory.incrementBloomFilterSaturatedDropped(saturatedDropped);
      }
      String bloomFileName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
      try (IndexOutput bloomOutput = state.directory.createOutput(bloomFileName, state.context)) {
        CodecUtil.writeIndexHeader(
            bloomOutput,
            BLOOM_CODEC_NAME,
            VERSION_CURRENT,
            state.segmentInfo.getId(),
            state.segmentSuffix);
        // remember the name of the postings format we will delegate to
        bloomOutput.writeString(delegatePostingsFormat.getName());

        // First field in the output file is the number of fields+blooms saved
        bloomOutput.writeInt(nonSaturatedBlooms.size());
        for (Entry<FieldInfo, MongotFuzzySet> entry : nonSaturatedBlooms) {
          FieldInfo fieldInfo = entry.getKey();
          MongotFuzzySet bloomFilter = entry.getValue();
          bloomOutput.writeInt(fieldInfo.number);
          saveAppropriatelySizedBloomFilter(bloomOutput, bloomFilter, fieldInfo);
        }
        CodecUtil.writeFooter(bloomOutput);
      }
      // We are done with large bitsets so no need to keep them hanging around
      bloomFilters.clear();
    }

    private void saveAppropriatelySizedBloomFilter(
        IndexOutput bloomOutput, MongotFuzzySet bloomFilter, FieldInfo fieldInfo)
        throws IOException {

      MongotFuzzySet rightSizedSet = bloomFilterFactory.downsize(fieldInfo, bloomFilter);
      if (rightSizedSet == null) {
        rightSizedSet = bloomFilter;
      }
      rightSizedSet.serialize(bloomOutput);
    }
  }

  @Override
  public String toString() {
    return "MongotBloomFilteringPostingsFormat(" + delegatePostingsFormat + ")";
  }
}
