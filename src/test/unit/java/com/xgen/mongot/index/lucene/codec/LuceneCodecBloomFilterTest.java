package com.xgen.mongot.index.lucene.codec;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat;
import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomReadPolicy;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.GlobalMetricFactory;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.bloom.DefaultBloomFilterFactory;
import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration tests that verify bloom filter postings format behavior at the Lucene segment level:
 * writing, reading, merging, dynamic toggling, and cross-read compatibility.
 */
@RunWith(Enclosed.class)
public class LuceneCodecBloomFilterTest {

  private static final String ID_FIELD = FieldName.MetaField.ID.getLuceneFieldName();
  private static final String BLOOM_FORMAT = MongotBloomFilteringPostingsFormat.BLOOM_CODEC_NAME;
  private static final String LUCENE99_FORMAT = "Lucene99";
  private static final String POSTINGS_FORMAT_ATTR = PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY;
  private static final String DOC_1 = "doc1";
  private static final String DOC_2 = "doc2";
  private static final String DOC_3 = "doc3";

  /** Bloom filter postings data file suffix (see {@link MongotBloomFilteringPostingsFormat}). */
  private static final String BLM_FILE_SUFFIX = ".blm";

  private static final boolean DFF_ENABLED = true;
  private static final boolean DFF_DISABLED = false;
  private static final boolean BLOOM = true;
  private static final boolean NON_BLOOM = false;
  private static final boolean CFS = true;
  private static final boolean NON_CFS = false;

  public static class BasicBloomFilterTests {

    /**
     * Critical check to validate that BloomFilteredTermsEnum being used prefers seek exact, which
     * is critical to bloom filter performance.
     */
    @Test
    public void bloomFilteredTermsEnum_prefersSeekExact() throws IOException {
      try (Directory dir = new ByteBuffersDirectory()) {
        writeDocuments(dir, () -> DFF_ENABLED, DOC_1);
        MongotBloomReadPolicy.setLoadBloomOnHeap(dir, true);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          for (LeafReaderContext ctx : reader.leaves()) {
            Terms terms = ctx.reader().terms(ID_FIELD);
            assertThat(terms).isNotNull();
            TermsEnum termsEnum = terms.iterator();
            assertThat(termsEnum.preferSeekExact()).isTrue();
          }
        } finally {
          MongotBloomReadPolicy.setLoadBloomOnHeap(dir, false);
        }
      }
    }

    @Test
    public void writeAndRead_bloomEnabled_segmentsHaveBloomFormat() throws IOException {
      try (Directory dir = new ByteBuffersDirectory()) {
        writeDocuments(dir, () -> DFF_ENABLED, DOC_1, DOC_2, DOC_3);

        List<String> formats = getIdPostingsFormatNames(dir);
        assertThat(formats).isNotEmpty();
        assertThat(formats).containsExactly(BLOOM_FORMAT);
        assertAllSegmentsHaveBlmFiles(dir, true);
        assertIdLookupFindsDoc(dir, DOC_1);
        assertIdLookupFindsDoc(dir, DOC_2);
        assertIdLookupFindsDoc(dir, DOC_3);
      }
    }

    @Test
    public void writeAndRead_bloomDisabled_segmentsHaveDefaultFormat() throws IOException {
      try (Directory dir = new ByteBuffersDirectory()) {
        writeDocuments(dir, () -> DFF_DISABLED, DOC_1, DOC_2, DOC_3);

        List<String> formats = getIdPostingsFormatNames(dir);
        assertThat(formats).isNotEmpty();
        for (String format : formats) {
          assertThat(format).isNotEqualTo(BLOOM_FORMAT);
        }
        assertAllSegmentsHaveBlmFiles(dir, false);
        assertIdLookupFindsDoc(dir, DOC_1);
        assertIdLookupFindsDoc(dir, DOC_2);
        assertIdLookupFindsDoc(dir, DOC_3);
      }
    }

    @Test
    public void writeBloomEnabled_readWithBloomDisabled_documentsReadable() throws IOException {
      try (Directory dir = new ByteBuffersDirectory()) {
        writeDocuments(dir, () -> DFF_ENABLED, DOC_1, DOC_2);
        assertThat(getIdPostingsFormatNames(dir)).containsExactly(BLOOM_FORMAT);

        try (IndexWriter writer = newWriter(dir, () -> DFF_DISABLED);
            DirectoryReader reader = DirectoryReader.open(writer)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, DOC_1)))).isEqualTo(1);
          assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, DOC_2)))).isEqualTo(1);
        }
        assertAllSegmentsHaveBlmFiles(dir, true);
      }
    }

    @Test
    public void writeBloomDisabled_readWithBloomEnabled_documentsReadable() throws IOException {
      try (Directory dir = new ByteBuffersDirectory()) {
        writeDocuments(dir, () -> DFF_DISABLED, DOC_1, DOC_2);
        for (String format : getIdPostingsFormatNames(dir)) {
          assertThat(format).isNotEqualTo(BLOOM_FORMAT);
        }

        try (IndexWriter writer = newWriter(dir, () -> DFF_ENABLED);
            DirectoryReader reader = DirectoryReader.open(writer)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, DOC_1)))).isEqualTo(1);
          assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, DOC_2)))).isEqualTo(1);
        }
        assertAllSegmentsHaveBlmFiles(dir, false);
      }
    }

    @Test
    public void bloomEnabledMidSegment_flush_producesBloomSegment() throws IOException {
      AtomicBoolean bloomEnabled = new AtomicBoolean(DFF_DISABLED);
      try (Directory dir = new ByteBuffersDirectory()) {
        try (IndexWriter writer = newWriter(dir, bloomEnabled::get)) {
          addDoc(writer, DOC_1);
          bloomEnabled.set(DFF_ENABLED);
          writer.commit();
        }

        List<String> formats = getIdPostingsFormatNames(dir);
        assertThat(formats).containsExactly(BLOOM_FORMAT);
        assertAllSegmentsHaveBlmFiles(dir, true);
        assertIdLookupFindsDoc(dir, DOC_1);
      }
    }

    @Test
    public void bloomDisabledMidSegment_flush_producesNonBloomSegment() throws IOException {
      AtomicBoolean bloomEnabled = new AtomicBoolean(DFF_ENABLED);
      try (Directory dir = new ByteBuffersDirectory()) {
        try (IndexWriter writer = newWriter(dir, bloomEnabled::get)) {
          addDoc(writer, DOC_1);
          bloomEnabled.set(DFF_DISABLED);
          writer.commit();
        }

        List<String> formats = getIdPostingsFormatNames(dir);
        assertThat(formats).hasSize(1);
        assertThat(formats.getFirst()).isNotEqualTo(BLOOM_FORMAT);
        assertAllSegmentsHaveBlmFiles(dir, false);
        assertIdLookupFindsDoc(dir, DOC_1);
      }
    }

    @Test
    public void writeWithBloomEnabled_withMetrics_recordsBloomPostingCounter() throws IOException {
      IndexMetricsUpdater.IndexingMetricsUpdater metricsUpdater =
          SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH);

      try (Directory dir = new ByteBuffersDirectory()) {
        IndexWriterConfig config =
            new IndexWriterConfig()
                .setCodec(
                    LuceneCodec.Factory.forIndexWithBloomFilter(
                        Map.of(), () -> DFF_ENABLED, Optional.of(metricsUpdater)));
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          addDoc(writer, DOC_1);
          addDoc(writer, DOC_2);
          writer.commit();
        }

        assertThat(metricsUpdater.getBloomFilterIdPostingCreatedCounter().count()).isEqualTo(1.0);
        assertThat(metricsUpdater.getLucene99IdPostingCreatedCounter().count()).isEqualTo(0.0);
        assertAllSegmentsHaveBlmFiles(dir, true);
        assertIdLookupFindsDoc(dir, DOC_1);
        assertIdLookupFindsDoc(dir, DOC_2);
      }
    }

    @Test
    public void writeWithBloomDisabled_withMetrics_recordsLucene99PostingCounter()
        throws IOException {
      IndexMetricsUpdater.IndexingMetricsUpdater metricsUpdater =
          SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH);

      try (Directory dir = new ByteBuffersDirectory()) {
        IndexWriterConfig config =
            new IndexWriterConfig()
                .setCodec(
                    LuceneCodec.Factory.forIndexWithBloomFilter(
                        Map.of(), () -> DFF_DISABLED, Optional.of(metricsUpdater)));
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          addDoc(writer, DOC_1);
          writer.commit();
        }

        assertThat(metricsUpdater.getLucene99IdPostingCreatedCounter().count()).isEqualTo(1.0);
        assertThat(metricsUpdater.getBloomFilterIdPostingCreatedCounter().count()).isEqualTo(0.0);
        assertAllSegmentsHaveBlmFiles(dir, false);
      }
    }

    @Test
    public void readWithBloomOnHeap_absentIdSkips_presentIdProbes() throws IOException {
      // Bloom lookup counters are published process-wide via GlobalMetricFactory; install a fresh
      // registry so this test observes only its own increments.
      SimpleMeterRegistry registry = new SimpleMeterRegistry();
      GlobalMetricFactory.resetForTest();
      GlobalMetricFactory.initialize(registry);

      try (Directory dir = new ByteBuffersDirectory()) {
        IndexWriterConfig config =
            new IndexWriterConfig()
                .setCodec(
                    LuceneCodec.Factory.forIndexWithBloomFilter(
                        Map.of(), () -> DFF_ENABLED, Optional.empty()))
                .setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          addDoc(writer, DOC_1);
          writer.commit();
        }

        double skipsBefore = counterCount(registry, "bloom_filter_id_lookup_skipped");
        double probesBefore = counterCount(registry, "bloom_filter_id_lookup_probed");
        double falsePositivesBefore =
            counterCount(registry, "bloom_filter_id_lookup_false_positive");

        MongotBloomReadPolicy.setLoadBloomOnHeap(dir, true);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, DOC_1)))).isEqualTo(1);
          assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, "missing")))).isEqualTo(0);
        } finally {
          MongotBloomReadPolicy.setLoadBloomOnHeap(dir, false);
        }

        double skipDelta = counterCount(registry, "bloom_filter_id_lookup_skipped") - skipsBefore;
        double probeDelta = counterCount(registry, "bloom_filter_id_lookup_probed") - probesBefore;
        double falsePositiveDelta =
            counterCount(registry, "bloom_filter_id_lookup_false_positive") - falsePositivesBefore;

        // The present id passes the filter and seeks the delegate (a probe, never a false
        // positive).
        assertThat(probeDelta).isAtLeast(1.0);
        // The absent id is normally ruled out in-heap (a skip). A bloom false positive for it is
        // rare but legal and depends on hashing/sizing, so accept either outcome rather than
        // pinning
        // an exact count that could drift across Lucene or bloom-parameter changes: the absent
        // lookup is either skipped or probed and recorded as a false positive.
        assertThat(skipDelta + falsePositiveDelta).isAtLeast(1.0);
        assertThat(falsePositiveDelta).isAtMost(1.0);
      }
    }

    @Test
    public void saturatedBloom_onClose_recordsDropped() throws IOException {
      // Saturated-dropped is published process-wide via GlobalMetricFactory.
      SimpleMeterRegistry registry = new SimpleMeterRegistry();
      GlobalMetricFactory.resetForTest();
      GlobalMetricFactory.initialize(registry);
      // A factory that always reports saturation forces close() to drop every accumulated filter.
      DefaultBloomFilterFactory alwaysSaturated =
          new DefaultBloomFilterFactory() {
            @Override
            public boolean isSaturated(FuzzySet bloomFilter, FieldInfo fieldInfo) {
              return true;
            }
          };
      PostingsFormat delegateId = new Lucene99Codec().getPostingsFormatForField(ID_FIELD);
      PostingsFormat bloomFormat =
          new MongotBloomFilteringPostingsFormat(delegateId, alwaysSaturated);

      try (Directory dir = new ByteBuffersDirectory()) {
        IndexWriterConfig config =
            new IndexWriterConfig().setCodec(perFieldCodec(bloomFormat)).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          addDoc(writer, DOC_1);
          writer.commit();
        }
        assertThat(counterCount(registry, "bloom_filter_saturated_dropped")).isAtLeast(1.0);
      }
    }
  }

  private static double counterCount(MeterRegistry registry, String name) {
    Counter counter = registry.find(name).counter();
    return counter == null ? 0.0 : counter.count();
  }

  @RunWith(Parameterized.class)
  public static class ForceMergeCompoundAndPostingsCombinationsTest {

    public record TestSpec(
        boolean bloomEnabledOnMerge,
        boolean seg1Compound,
        boolean seg1Bloom,
        boolean seg2Compound,
        boolean seg2Bloom,
        String expectedFormat) {
      @Override
      public String toString() {
        return String.format(
            "With DFF=%s, merging seg1(%s,%s) + seg2(%s,%s) -> %s",
            this.bloomEnabledOnMerge ? "BLOOM" : "NON_BLOOM",
            this.seg1Compound ? "CFS" : "NON_CFS",
            this.seg1Bloom ? "BLOOM" : "NON_BLOOM",
            this.seg2Compound ? "CFS" : "NON_CFS",
            this.seg2Bloom ? "BLOOM" : "NON_BLOOM",
            this.expectedFormat);
      }
    }

    private final TestSpec testSpec;

    public ForceMergeCompoundAndPostingsCombinationsTest(TestSpec testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestSpec> data() {
      return List.of(
          // Bloom enabled on merge, both compound
          new TestSpec(DFF_ENABLED, CFS, BLOOM, CFS, BLOOM, BLOOM_FORMAT),
          new TestSpec(DFF_ENABLED, CFS, BLOOM, CFS, NON_BLOOM, BLOOM_FORMAT),
          new TestSpec(DFF_ENABLED, CFS, NON_BLOOM, CFS, BLOOM, BLOOM_FORMAT),
          new TestSpec(DFF_ENABLED, CFS, NON_BLOOM, CFS, NON_BLOOM, BLOOM_FORMAT),
          // Bloom enabled on merge, both non-compound
          new TestSpec(DFF_ENABLED, NON_CFS, BLOOM, NON_CFS, BLOOM, BLOOM_FORMAT),
          new TestSpec(DFF_ENABLED, NON_CFS, BLOOM, NON_CFS, NON_BLOOM, BLOOM_FORMAT),
          new TestSpec(DFF_ENABLED, NON_CFS, NON_BLOOM, NON_CFS, BLOOM, BLOOM_FORMAT),
          new TestSpec(DFF_ENABLED, NON_CFS, NON_BLOOM, NON_CFS, NON_BLOOM, BLOOM_FORMAT),
          // Bloom enabled on merge, compound + non-compound
          new TestSpec(DFF_ENABLED, CFS, BLOOM, NON_CFS, BLOOM, BLOOM_FORMAT),
          new TestSpec(DFF_ENABLED, CFS, BLOOM, NON_CFS, NON_BLOOM, BLOOM_FORMAT),
          new TestSpec(DFF_ENABLED, CFS, NON_BLOOM, NON_CFS, BLOOM, BLOOM_FORMAT),
          new TestSpec(DFF_ENABLED, CFS, NON_BLOOM, NON_CFS, NON_BLOOM, BLOOM_FORMAT),
          // Bloom disabled on merge, both compound
          new TestSpec(DFF_DISABLED, CFS, BLOOM, CFS, BLOOM, LUCENE99_FORMAT),
          new TestSpec(DFF_DISABLED, CFS, BLOOM, CFS, NON_BLOOM, LUCENE99_FORMAT),
          new TestSpec(DFF_DISABLED, CFS, NON_BLOOM, CFS, BLOOM, LUCENE99_FORMAT),
          new TestSpec(DFF_DISABLED, CFS, NON_BLOOM, CFS, NON_BLOOM, LUCENE99_FORMAT),
          // Bloom disabled on merge, both non-compound
          new TestSpec(DFF_DISABLED, NON_CFS, BLOOM, NON_CFS, BLOOM, LUCENE99_FORMAT),
          new TestSpec(DFF_DISABLED, NON_CFS, BLOOM, NON_CFS, NON_BLOOM, LUCENE99_FORMAT),
          new TestSpec(DFF_DISABLED, NON_CFS, NON_BLOOM, NON_CFS, BLOOM, LUCENE99_FORMAT),
          new TestSpec(DFF_DISABLED, NON_CFS, NON_BLOOM, NON_CFS, NON_BLOOM, LUCENE99_FORMAT),
          // Bloom disabled on merge, compound + non-compound
          new TestSpec(DFF_DISABLED, CFS, BLOOM, NON_CFS, BLOOM, LUCENE99_FORMAT),
          new TestSpec(DFF_DISABLED, CFS, BLOOM, NON_CFS, NON_BLOOM, LUCENE99_FORMAT),
          new TestSpec(DFF_DISABLED, CFS, NON_BLOOM, NON_CFS, BLOOM, LUCENE99_FORMAT),
          new TestSpec(DFF_DISABLED, CFS, NON_BLOOM, NON_CFS, NON_BLOOM, LUCENE99_FORMAT));
    }

    @Test
    public void forceMerge_producesExpectedFormat() throws IOException {
      assertForceMergeProducesReadableExpectedFormat(
          this.testSpec.bloomEnabledOnMerge(),
          this.testSpec.seg1Compound(),
          this.testSpec.seg1Bloom(),
          this.testSpec.seg2Compound(),
          this.testSpec.seg2Bloom(),
          this.testSpec.expectedFormat());
    }
  }

  private static IndexWriter newWriter(Directory dir, BooleanSupplier bloomSupplier)
      throws IOException {
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(
                LuceneCodec.Factory.forIndexWithBloomFilter(
                    Map.of(), bloomSupplier, Optional.empty()));
    return new IndexWriter(dir, config);
  }

  private static void writeDocuments(Directory dir, BooleanSupplier bloomSupplier, String... ids)
      throws IOException {
    try (IndexWriter writer = newWriter(dir, bloomSupplier)) {
      for (String id : ids) {
        addDoc(writer, id);
      }
      writer.commit();
    }
  }

  private static void addDoc(IndexWriter writer, String id) throws IOException {
    Document doc = new Document();
    doc.add(new StringField(ID_FIELD, id, Field.Store.YES));
    writer.addDocument(doc);
  }

  /** Per-field codec that routes the {@code _id} field to the supplied postings format. */
  private static Codec perFieldCodec(PostingsFormat idPostingsFormat) {
    Lucene99Codec delegateCodec = new Lucene99Codec();
    PerFieldPostingsFormat perField =
        new PerFieldPostingsFormat() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return ID_FIELD.equals(field)
                ? idPostingsFormat
                : delegateCodec.getPostingsFormatForField(field);
          }
        };
    return new FilterCodec(LUCENE99_FORMAT, delegateCodec) {
      @Override
      public PostingsFormat postingsFormat() {
        return perField;
      }
    };
  }

  private static List<String> getIdPostingsFormatNames(Directory dir) throws IOException {
    List<String> names = new ArrayList<>();
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      for (LeafReaderContext ctx : reader.leaves()) {
        FieldInfo idInfo = ctx.reader().getFieldInfos().fieldInfo(ID_FIELD);
        if (idInfo != null) {
          names.add(idInfo.getAttribute(POSTINGS_FORMAT_ATTR));
        }
      }
    }
    return names;
  }

  private static void assertIdLookupFindsDoc(Directory dir, String id) throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      assertThat(searcher.count(new TermQuery(new Term(ID_FIELD, id)))).isEqualTo(1);
    }
  }

  /**
   * Asserts whether each segment contains a bloom filter {@code .blm} file. When compound files are
   * enabled, {@code .blm} is stored inside the {@code .cfs} stream; the check uses the segment
   * codec's compound reader or {@link SegmentInfo#files()} as appropriate.
   */
  private static void assertAllSegmentsHaveBlmFiles(Directory dir, boolean expectBlm)
      throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      assertThat(reader.leaves()).isNotEmpty();
      for (LeafReaderContext ctx : reader.leaves()) {
        SegmentReader segReader = (SegmentReader) ctx.reader();
        boolean hasBlm = segmentContainsBlmFile(dir, segReader);
        assertWithMessage("BLM file presence for segment %s", segReader.getSegmentInfo().info.name)
            .that(hasBlm)
            .isEqualTo(expectBlm);
      }
    }
  }

  private static boolean segmentContainsBlmFile(Directory dir, SegmentReader segReader)
      throws IOException {
    SegmentInfo si = segReader.getSegmentInfo().info;
    Codec codec = si.getCodec();
    if (si.getUseCompoundFile()) {
      try (CompoundDirectory compoundDir = codec.compoundFormat().getCompoundReader(dir, si)) {
        for (String name : compoundDir.listAll()) {
          if (name.endsWith(BLM_FILE_SUFFIX)) {
            return true;
          }
        }
      }
    } else {
      for (String file : si.files()) {
        if (file.endsWith(BLM_FILE_SUFFIX)) {
          return true;
        }
      }
    }
    return false;
  }

  private static void assertForceMergeProducesReadableExpectedFormat(
      boolean bloomEnabledOnMerge,
      boolean seg1Compound,
      boolean seg1Bloom,
      boolean seg2Compound,
      boolean seg2Bloom,
      String expectedFormat)
      throws IOException {
    AtomicBoolean bloomEnabled = new AtomicBoolean();

    try (Directory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(
                  LuceneCodec.Factory.forIndexWithBloomFilter(
                      Map.of(), bloomEnabled::get, Optional.empty()));

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        bloomEnabled.set(seg1Bloom);
        writer.getConfig().setUseCompoundFile(seg1Compound);
        addDoc(writer, DOC_1);
        writer.commit();

        bloomEnabled.set(seg2Bloom);
        writer.getConfig().setUseCompoundFile(seg2Compound);
        addDoc(writer, DOC_2);
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          List<LeafReaderContext> leaves = reader.leaves();
          assertThat(leaves).hasSize(2);
          assertSegmentProperties(leaves.get(0), seg1Compound, seg1Bloom, "seg1");
          assertSegmentProperties(leaves.get(1), seg2Compound, seg2Bloom, "seg2");
        }

        bloomEnabled.set(bloomEnabledOnMerge);
        writer.forceMerge(1);
        writer.commit();
      }

      List<String> formats = getIdPostingsFormatNames(dir);
      assertThat(formats).containsExactly(expectedFormat);
      assertAllSegmentsHaveBlmFiles(dir, BLOOM_FORMAT.equals(expectedFormat));
      assertIdLookupFindsDoc(dir, DOC_1);
      assertIdLookupFindsDoc(dir, DOC_2);
    }
  }

  private static void assertSegmentProperties(
      LeafReaderContext ctx, boolean expectedCompound, boolean expectedBloom, String segmentLabel) {
    SegmentReader segReader = (SegmentReader) ctx.reader();
    assertWithMessage("Compound file for " + segmentLabel)
        .that(segReader.getSegmentInfo().info.getUseCompoundFile())
        .isEqualTo(expectedCompound);
    FieldInfo idInfo = segReader.getFieldInfos().fieldInfo(ID_FIELD);
    String formatName = idInfo.getAttribute(POSTINGS_FORMAT_ATTR);
    assertWithMessage("Postings format for " + segmentLabel)
        .that(formatName)
        .isEqualTo(expectedBloom ? BLOOM_FORMAT : LUCENE99_FORMAT);
  }
}
