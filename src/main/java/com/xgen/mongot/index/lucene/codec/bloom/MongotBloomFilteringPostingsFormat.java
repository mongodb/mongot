/*
 * Derived from lucene-mongot 10.1.0 BloomFilteringPostingsFormat.
 */

package com.xgen.mongot.index.lucene.codec.bloom;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
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
import org.apache.lucene.codecs.bloom.BloomFilterFactory;
import org.apache.lucene.codecs.bloom.DefaultBloomFilterFactory;
import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.codecs.bloom.FuzzySet.ContainsResult;
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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.CompiledAutomaton;

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
 * was written with {@link #BLOOM_CODEC_NAME}. Whether deserialized bloom bitsets are kept on heap
 * is controlled separately by {@link MongotBloomReadPolicy} (also driven by the same feature-flag
 * policy as the write supplier).
 *
 * <p><b>Differences from upstream.</b>
 *
 * <ul>
 *   <li><b>SPI / codec name</b> — {@link #BLOOM_CODEC_NAME} is {@code MongotBloomFilter} instead of
 *       upstream {@code BloomFilter}, so this class is registered separately from Lucene's built-in
 *       format and segments written here store the mongot name in the index header.
 *   <li><b>Conditional bloom heap load on read</b> — {@link
 *       MongotBloomReadPolicy#shouldLoadBloomOnRead()} controls whether {@link FuzzySet} bitsets
 *       are deserialized into heap. When false, {@link #fieldsProducer} returns the delegate {@link
 *       FieldsProducer} directly: only the delegate name is read from the {@code .blm} header (no
 *       bloom bitsets loaded); term lookups use delegate postings only (no in-memory fast-fail).
 *   <li><b>Heap eviction on close</b> — {@link MongotBloomFilteredFieldsProducer#close()} clears
 *       {@code bloomsByFieldName} before closing the delegate producer so in-heap bloom state is
 *       dropped when the segment reader is closed.
 * </ul>
 *
 * <p>A choice of {@link BloomFilterFactory} can be passed to tailor Bloom Filter settings on a
 * per-field basis. The default configuration is {@link DefaultBloomFilterFactory} which allocates a
 * ~8mb bitset and hashes values using {@link MurmurHash64}. This should be suitable for most
 * purposes.
 *
 * <p>The format of the blm file is as follows:
 *
 * <ul>
 *   <li>BloomFilter (.blm) --&gt; Header, DelegatePostingsFormatName, NumFilteredFields,
 *       Filter<sup>NumFilteredFields</sup>, Footer
 *   <li>Filter --&gt; FieldNumber, FuzzySet
 *   <li>FuzzySet --&gt;See {@link FuzzySet#serialize(DataOutput)}
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

  private final BloomFilterFactory bloomFilterFactory;
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
   * @param bloomFilterFactory The {@link BloomFilterFactory} responsible for sizing BloomFilters
   *     appropriately
   */
  public MongotBloomFilteringPostingsFormat(
      PostingsFormat delegatePostingsFormat, BloomFilterFactory bloomFilterFactory) {
    super(BLOOM_CODEC_NAME);
    this.delegatePostingsFormat = delegatePostingsFormat;
    this.bloomFilterFactory = bloomFilterFactory;
  }

  /**
   * Creates Bloom filters for a selection of fields created in the index. This is recorded as a set
   * of Bitsets held as a segment summary in an additional "blm" file. This PostingsFormat delegates
   * to a choice of delegate PostingsFormat for encoding all other postings data. This choice of
   * constructor defaults to the {@link DefaultBloomFilterFactory} for configuring per-field
   * BloomFilters.
   *
   * @param delegatePostingsFormat The PostingsFormat that records all the non-bloom filter data
   *     i.e. postings info.
   */
  public MongotBloomFilteringPostingsFormat(PostingsFormat delegatePostingsFormat) {
    this(delegatePostingsFormat, new DefaultBloomFilterFactory());
  }

  // Used only at read-time via Service Provider instantiation -
  // do not use at Write-time in application code.
  public MongotBloomFilteringPostingsFormat() {
    this(null, new DefaultBloomFilterFactory());
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

  static class MongotBloomFilteredFieldsProducer extends FieldsProducer {
    private FieldsProducer delegateFieldsProducer;
    private final Map<String, FuzzySet> bloomsByFieldName = new ConcurrentHashMap<>();
    private final List<MongotBloomFieldsProducerRegistry.Key> registryKeys = new ArrayList<>();

    public MongotBloomFilteredFieldsProducer(SegmentReadState state) throws IOException {

      ChecksumIndexInput bloomIn = null;
      boolean success = false;
      try {
        bloomIn = openBlmChecksumInput(state);
        // // Load the hash function used in the BloomFilter
        // hashFunction = HashFunction.forName(bloomIn.readString());
        // Load the delegate postings format
        PostingsFormat delegateFormat = PostingsFormat.forName(bloomIn.readString());

        this.delegateFieldsProducer = delegateFormat.fieldsProducer(state);
        int numBlooms = bloomIn.readInt();
        for (int i = 0; i < numBlooms; i++) {
          int fieldNum = bloomIn.readInt();
          FuzzySet bloom = FuzzySet.deserialize(bloomIn);
          FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
          bloomsByFieldName.put(fieldInfo.name, bloom);
        }
        CodecUtil.checkFooter(bloomIn);
        IOUtils.close(bloomIn);
        for (String fieldName : bloomsByFieldName.keySet()) {
          registryKeys.add(MongotBloomFieldsProducerRegistry.register(state, fieldName, this));
        }
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
      delegateFieldsProducer.close();
    }

    @Override
    public Terms terms(String field) throws IOException {
      FuzzySet filter = bloomsByFieldName.get(field);
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
    FuzzySet getHeapLoadedBloomForField(String fieldName) {
      return bloomsByFieldName.get(fieldName);
    }

    /** True if this segment currently holds any deserialized bloom bitsets on heap. */
    boolean hasHeapLoadedBloom() {
      return !bloomsByFieldName.isEmpty();
    }

    /** Estimated heap bytes held by this segment's deserialized bloom bitsets. */
    long heapBloomRamBytesUsed() {
      @Var long total = 0;
      for (FuzzySet bloom : bloomsByFieldName.values()) {
        total += bloom.ramBytesUsed();
      }
      return total;
    }

    /**
     * Drops in-heap bloom bitsets while keeping the delegate postings reader open.
     *
     * <p>Safe to call while other threads search the same segment: {@link #terms} only reads the
     * map, and in-flight {@link MongotBloomFilteredTerms} instances retain their {@link FuzzySet}
     * reference. {@link ConcurrentHashMap} makes {@code clear()} safe with concurrent {@code get}.
     */
    void evictHeapBloom() {
      bloomsByFieldName.clear();
    }

    static class MongotBloomFilteredTerms extends Terms {
      private Terms delegateTerms;
      private FuzzySet filter;

      public MongotBloomFilteredTerms(Terms terms, FuzzySet filter) {
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
      private final FuzzySet filter;

      public MongotBloomFilteredTermsEnum(Terms delegateTerms, FuzzySet filter) throws IOException {
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
    private Map<FieldInfo, FuzzySet> bloomFilters = new HashMap<>();
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

        FuzzySet bloomFilter = null;

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
      List<Entry<FieldInfo, FuzzySet>> nonSaturatedBlooms = new ArrayList<>();

      for (Entry<FieldInfo, FuzzySet> entry : bloomFilters.entrySet()) {
        FuzzySet bloomFilter = entry.getValue();
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
        for (Entry<FieldInfo, FuzzySet> entry : nonSaturatedBlooms) {
          FieldInfo fieldInfo = entry.getKey();
          FuzzySet bloomFilter = entry.getValue();
          bloomOutput.writeInt(fieldInfo.number);
          saveAppropriatelySizedBloomFilter(bloomOutput, bloomFilter, fieldInfo);
        }
        CodecUtil.writeFooter(bloomOutput);
      }
      // We are done with large bitsets so no need to keep them hanging around
      bloomFilters.clear();
    }

    private void saveAppropriatelySizedBloomFilter(
        IndexOutput bloomOutput, FuzzySet bloomFilter, FieldInfo fieldInfo) throws IOException {

      FuzzySet rightSizedSet = bloomFilterFactory.downsize(fieldInfo, bloomFilter);
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
