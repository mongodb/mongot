package com.xgen.mongot.index.lucene.codec.bloom;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.codec.bloom.MongotBloomFilteringPostingsFormat.MongotBloomFilteredFieldsProducer;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.junit.Test;

/** Bloom eviction tests for {@link MongotBloomFilteringPostingsFormat}. */
public class MongotBloomEvictionTest {

  private static final String ID_FIELD = FieldName.MetaField.ID.getLuceneFieldName();

  private static final MongotBloomFilterFactory SMALL_BLOOM_FILTER_FACTORY =
      new MongotBloomFilterFactory() {
        @Override
        public MongotFuzzySet getSetForField(SegmentWriteState state, FieldInfo fieldInfo) {
          if (!ID_FIELD.equals(fieldInfo.name)) {
            return super.getSetForField(state, fieldInfo);
          }
          return MongotFuzzySet.createSetBasedOnMaxMemory(4096);
        }
      };

  @Test
  public void bloomEviction_readerClose_dropsBloomFromProducer() throws IOException {
    MongotBloomFilteredFieldsProducer bloomProducer;

    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, smallBloomWriterConfig())) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, "doc1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
      }

      MongotBloomReadPolicy.setLoadBloomOnRead(directory, true);
      try {
        try (DirectoryReader bloomReader = DirectoryReader.open(directory)) {
          SegmentReader segment = (SegmentReader) bloomReader.leaves().get(0).reader();
          assertThat(getLoadedBloomForField(segment, ID_FIELD)).isPresent();
          bloomProducer = unwrapBloomFieldsProducer(segment, ID_FIELD).orElseThrow();
          assertThat(bloomProducer.getLoadedBloomForField(ID_FIELD) != null).isTrue();
        }
      } finally {
        MongotBloomReadPolicy.setLoadBloomOnRead(directory, false);
      }
    }

    // Producer outlives the reader; close() must drop the loaded bloom sets without relying on GC.
    assertThat(bloomProducer.getLoadedBloomForField(ID_FIELD) != null).isFalse();
  }

  @Test
  public void perDirectoryPolicy_isIndependent() throws IOException {
    try (Directory directoryA = new ByteBuffersDirectory();
        Directory directoryB = new ByteBuffersDirectory()) {
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(directoryA)).isFalse();

      MongotBloomReadPolicy.setLoadBloomOnRead(directoryA, true);
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(directoryA)).isTrue();
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(directoryB)).isFalse();

      MongotBloomReadPolicy.setLoadBloomOnRead(directoryB, true);
      MongotBloomReadPolicy.setLoadBloomOnRead(directoryA, false);
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(directoryA)).isFalse();
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(directoryB)).isTrue();
    }
  }

  @Test
  public void blmSidecar_recordsDelegatePostingsFormatNameFromWritePath() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, smallBloomWriterConfig())) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, "doc1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
        String recordedDelegateName = readDelegatePostingsFormatNameFromBlm(directory, segment);
        assertThat(recordedDelegateName).isEqualTo(expectedDelegatePostingsFormatName());
      }
    }
  }

  @Test
  public void bloomEviction_steadyStateRead_skipsBloom() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, smallBloomWriterConfig())) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, "doc1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
        assertThat(unwrapBloomFieldsProducer(segment, ID_FIELD)).isEmpty();
        assertThat(getLoadedBloomForField(segment, ID_FIELD)).isEmpty();
        assertThat(segment.terms(ID_FIELD)).isNotNull();
      }
    }
  }

  private static String expectedDelegatePostingsFormatName() {
    return new Lucene99Codec().getPostingsFormatForField(ID_FIELD).getName();
  }

  private static IndexWriterConfig smallBloomWriterConfig() {
    Lucene99Codec delegateCodec = new Lucene99Codec();
    PostingsFormat delegateIdPostingsFormat = delegateCodec.getPostingsFormatForField(ID_FIELD);
    PostingsFormat bloomIdPostingsFormat =
        new MongotBloomFilteringPostingsFormat(
            delegateIdPostingsFormat, SMALL_BLOOM_FILTER_FACTORY);
    PerFieldPostingsFormat perFieldPostingsFormat =
        new PerFieldPostingsFormat() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return ID_FIELD.equals(field)
                ? bloomIdPostingsFormat
                : delegateCodec.getPostingsFormatForField(field);
          }
        };

    org.apache.lucene.codecs.Codec codec =
        new FilterCodec("Lucene99", delegateCodec) {
          @Override
          public PostingsFormat postingsFormat() {
            return perFieldPostingsFormat;
          }
        };
    return new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
  }

  private static String readDelegatePostingsFormatNameFromBlm(
      Directory directory, SegmentReader segment) throws IOException {
    SegmentInfo segmentInfo = segment.getSegmentInfo().info;
    String bloomExtension = "." + MongotBloomFilteringPostingsFormat.BLOOM_EXTENSION;
    String segmentNamePrefix = segmentInfo.name + "_";
    String bloomFileName =
        java.util.Arrays.stream(directory.listAll())
            .filter(name -> name.startsWith(segmentNamePrefix) && name.endsWith(bloomExtension))
            .findFirst()
            .orElseThrow(
                () ->
                    new IOException(
                        "No "
                            + bloomExtension
                            + " file in directory for segment "
                            + segmentInfo.name));
    String segmentSuffix =
        bloomFileName.substring(
            segmentNamePrefix.length(), bloomFileName.length() - bloomExtension.length());
    try (ChecksumIndexInput bloomIn = directory.openChecksumInput(bloomFileName)) {
      CodecUtil.checkIndexHeader(
          bloomIn,
          MongotBloomFilteringPostingsFormat.BLOOM_CODEC_NAME,
          MongotBloomFilteringPostingsFormat.VERSION_START,
          MongotBloomFilteringPostingsFormat.VERSION_CURRENT,
          segmentInfo.getId(),
          segmentSuffix);
      return bloomIn.readString();
    }
  }

  private static Optional<MongotFuzzySet> getLoadedBloomForField(
      SegmentReader reader, String field) {
    return unwrapBloomFieldsProducer(reader, field)
        .map(producer -> producer.getLoadedBloomForField(field));
  }

  private static Optional<MongotBloomFilteredFieldsProducer> unwrapBloomFieldsProducer(
      SegmentReader segment, String field) {
    FieldsProducer producer = segment.getPostingsReader();
    if (producer instanceof MongotBloomFilteredFieldsProducer bloomProducer) {
      return Optional.of(bloomProducer);
    }
    return MongotBloomFieldsProducerRegistry.lookup(segment, field);
  }
}
