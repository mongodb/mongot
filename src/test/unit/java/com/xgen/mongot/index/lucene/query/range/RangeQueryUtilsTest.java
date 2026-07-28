package com.xgen.mongot.index.lucene.query.range;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.StringRangeBound;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.StringPoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Test;

/**
 * Direct tests for the single-valued checks in {@link RangeQueryUtils}. Each test builds a small
 * on-disk Lucene index that exercises one branch: empty index, scalar (single-valued) data,
 * multi-valued data, mixed segments, a field that the index does not carry, a document whose
 * numeric values are split across the int64 and double point-field variants, single-valued
 * documents split across the variants (admitted by the exact docID-disjointness check, cached per
 * segment core), date data, and embedded-root field-name selection.
 */
public class RangeQueryUtilsTest {

  private static final String FIELD_NAME = "testField";

  @Test
  public void emptyIndex_returnsTrue() throws IOException {
    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        assertTrue(RangeQueryUtils.hasNoArrayDataInFields(reader, List.of(FIELD_NAME)));
      }
    }
  }

  @Test
  public void singleValuePerDoc_returnsTrue() throws IOException {
    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 10; i++) {
          Document doc = new Document();
          doc.add(new LongPoint(FIELD_NAME, i));
          writer.addDocument(doc);
        }
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        assertTrue(RangeQueryUtils.hasNoArrayDataInFields(reader, List.of(FIELD_NAME)));
      }
    }
  }

  @Test
  public void multipleValuesPerDoc_returnsFalse() throws IOException {
    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new LongPoint(FIELD_NAME, 1L));
        doc.add(new LongPoint(FIELD_NAME, 2L));
        writer.addDocument(doc);
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        assertFalse(RangeQueryUtils.hasNoArrayDataInFields(reader, List.of(FIELD_NAME)));
      }
    }
  }

  @Test
  public void mixedSegmentsWithMultiValuedSegment_returnsFalse() throws IOException {
    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        // First segment: single-valued only.
        Document singleDoc = new Document();
        singleDoc.add(new LongPoint(FIELD_NAME, 1L));
        writer.addDocument(singleDoc);
        writer.commit();

        // Second segment: introduces a multi-valued document. Even one segment with multi-valued
        // data has to flip the answer to false — fusion would be unsound for this index.
        Document multiDoc = new Document();
        multiDoc.add(new LongPoint(FIELD_NAME, 10L));
        multiDoc.add(new LongPoint(FIELD_NAME, 20L));
        writer.addDocument(multiDoc);
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        assertFalse(RangeQueryUtils.hasNoArrayDataInFields(reader, List.of(FIELD_NAME)));
      }
    }
  }

  @Test
  public void nonexistentField_returnsTrue() throws IOException {
    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new LongPoint(FIELD_NAME, 1L));
        writer.addDocument(doc);
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        // A field that has no point values cannot contribute multi-valued documents.
        assertTrue(RangeQueryUtils.hasNoArrayDataInFields(reader, List.of("nonexistent")));
      }
    }
  }

  /**
   * Numeric ingestion routes each value to exactly one point-field variant by BSON type: integral
   * values to the int64 multi-value field, floating-point values to the double multi-value field. A
   * document like {@code {a: [1, 2.5]}} therefore contributes one point to each variant. Each
   * variant is single-valued when inspected on its own, but the document is multi-valued, so fusing
   * a pair of open bounds into one closed range is unsound and the merge check must say no.
   */
  @Test
  public void mixedIntDoubleArrayDocument_isNotSingleValuedForRangeMerge() throws IOException {
    FieldPath path = FieldPath.parse("a");
    String int64Field =
        FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(path, Optional.empty());
    String doubleField =
        FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(path, Optional.empty());

    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        // {a: [1, 2.5]} — one point in each numeric variant, as ingestion writes it.
        Document doc = new Document();
        doc.add(new LongPoint(int64Field, 1L));
        doc.add(new LongPoint(doubleField, LuceneDoubleConversionUtils.toLong(2.5)));
        writer.addDocument(doc);
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        NumericRangeBound bounds =
            new NumericRangeBound(
                Optional.of(new DoublePoint(2.0)), Optional.of(new DoublePoint(2.4)), false, false);
        assertFalse(
            "a document with values split across the int64 and double variants is multi-valued",
            RangeQueryUtils.hasNoArrayData(reader, bounds, List.of(path), Optional.empty()));
      }
    }
  }

  @Test
  public void singleValuedDateData_hasNoArrayData() throws IOException {
    FieldPath path = FieldPath.parse("a");
    String dateField = FieldName.TypeField.DATE_MULTIPLE.getLuceneFieldName(path, Optional.empty());

    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        // {a: Date(1500)} — one epoch-millis long point, as date ingestion writes it.
        Document doc = new Document();
        doc.add(new LongPoint(dateField, 1_500L));
        writer.addDocument(doc);
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        assertTrue(
            RangeQueryUtils.hasNoArrayData(reader, dateBounds(), List.of(path), Optional.empty()));
      }
    }
  }

  @Test
  public void multiValuedDateData_isNotSingleValuedForRangeMerge() throws IOException {
    FieldPath path = FieldPath.parse("a");
    String dateField = FieldName.TypeField.DATE_MULTIPLE.getLuceneFieldName(path, Optional.empty());

    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        // {a: [Date(500), Date(2500)]} — two points in the date field for one document.
        Document doc = new Document();
        doc.add(new LongPoint(dateField, 500L));
        doc.add(new LongPoint(dateField, 2_500L));
        writer.addDocument(doc);
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        assertFalse(
            RangeQueryUtils.hasNoArrayData(reader, dateBounds(), List.of(path), Optional.empty()));
      }
    }
  }

  /**
   * {@code embeddedRoot} selects which Lucene field names are inspected: multi-valued points stored
   * under the embedded variant of the field must fail the check when the query runs under that
   * embedded root, while the same index passes for {@link Optional#empty()} because the
   * non-embedded variant stores no points at all.
   */
  @Test
  public void embeddedRootSelectsInspectedFields() throws IOException {
    FieldPath path = FieldPath.parse("emb.a");
    Optional<FieldPath> embeddedRoot = Optional.of(FieldPath.parse("emb"));
    String embeddedInt64Field =
        FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(path, embeddedRoot);
    NumericRangeBound bounds =
        new NumericRangeBound(
            Optional.of(new DoublePoint(2.0)), Optional.of(new DoublePoint(2.4)), false, false);

    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new LongPoint(embeddedInt64Field, 1L));
        doc.add(new LongPoint(embeddedInt64Field, 2L));
        writer.addDocument(doc);
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        assertFalse(
            "the embedded variant of the field stores multi-valued points",
            RangeQueryUtils.hasNoArrayData(reader, bounds, List.of(path), embeddedRoot));
        assertTrue(
            "the non-embedded variant of the field stores no points",
            RangeQueryUtils.hasNoArrayData(reader, bounds, List.of(path), Optional.empty()));
      }
    }
  }

  private static DateRangeBound dateBounds() {
    return new DateRangeBound(
        Optional.of(new DatePoint(new Date(1_000L))),
        Optional.of(new DatePoint(new Date(2_000L))),
        false,
        false);
  }

  @Test
  public void hasNoArrayData_unsupportedBoundType_throws() throws IOException {
    StringRangeBound bounds =
        new StringRangeBound(
            Optional.of(new StringPoint("a")), Optional.of(new StringPoint("b")), true, true);

    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        writer.commit();
      }
      try (var reader = DirectoryReader.open(dir)) {
        IllegalStateException e =
            assertThrows(
                IllegalStateException.class,
                () ->
                    RangeQueryUtils.hasNoArrayData(
                        reader, bounds, List.of(FieldPath.parse("a")), Optional.empty()));
        assertThat(e).hasMessageThat().contains("StringRangeBound");
      }
    }
  }

  /**
   * Two documents in the same segment, one holding an integral value and one a floating-point value
   * at the same path. Each document is single-valued, but the summary statistics alone cannot prove
   * it, because both numeric variants store points in the segment. The exact per-segment
   * docID-disjointness check must admit this common shape.
   */
  @Test
  public void mixedIntDoubleScalarDocuments_hasNoArrayData() throws IOException {
    FieldPath path = FieldPath.parse("a");

    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      writeMixedIntDoubleScalarDocuments(dir, path);
      try (var reader = DirectoryReader.open(dir)) {
        assertTrue(
            "single-valued documents split across the numeric variants must be admitted",
            RangeQueryUtils.hasNoArrayData(
                reader, numericBounds(), List.of(path), Optional.empty()));
      }
    }
  }

  /**
   * The disjointness verification scans point docIDs at most once per segment core: the first call
   * pays the traversals, later calls are answered from the cache keyed by the segment's core cache
   * key.
   */
  @Test
  public void disjointnessCheck_isCachedPerSegmentCore() throws IOException {
    FieldPath path = FieldPath.parse("a");
    AtomicInteger traversals = new AtomicInteger();

    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      writeMixedIntDoubleScalarDocuments(dir, path);
      try (DirectoryReader reader =
          countingPointTraversalReader(DirectoryReader.open(dir), traversals)) {
        assertTrue(
            RangeQueryUtils.hasNoArrayData(
                reader, numericBounds(), List.of(path), Optional.empty()));
        int firstCallTraversals = traversals.get();
        assertTrue("the first call must scan point docIDs", firstCallTraversals > 0);

        assertTrue(
            RangeQueryUtils.hasNoArrayData(
                reader, numericBounds(), List.of(path), Optional.empty()));
        assertEquals(
            "the second call must be answered from the per-core cache without scanning",
            firstCallTraversals,
            traversals.get());
      }
    }
  }

  /**
   * A leaf without a core cache key gives the disjointness check no identity to cache its answer
   * under, so it refuses fusion instead of rescanning on every query — even when the data is
   * genuinely disjoint.
   */
  @Test
  public void nullCoreCacheKey_isNotSingleValuedForRangeMerge() throws IOException {
    FieldPath path = FieldPath.parse("a");

    try (Directory dir = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      writeMixedIntDoubleScalarDocuments(dir, path);
      try (DirectoryReader reader = nullCacheKeyReader(DirectoryReader.open(dir))) {
        assertFalse(
            "a leaf without a core cache key must be refused conservatively",
            RangeQueryUtils.hasNoArrayData(
                reader, numericBounds(), List.of(path), Optional.empty()));
      }
    }
  }

  private static NumericRangeBound numericBounds() {
    return new NumericRangeBound(
        Optional.of(new DoublePoint(2.0)), Optional.of(new DoublePoint(2.4)), false, false);
  }

  /** Writes {@code {a: 1}} and {@code {a: 2.5}} into one segment, as ingestion routes them. */
  private static void writeMixedIntDoubleScalarDocuments(Directory dir, FieldPath path)
      throws IOException {
    String int64Field =
        FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(path, Optional.empty());
    String doubleField =
        FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(path, Optional.empty());
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      Document intDoc = new Document();
      intDoc.add(new LongPoint(int64Field, 1L));
      writer.addDocument(intDoc);
      Document doubleDoc = new Document();
      doubleDoc.add(new LongPoint(doubleField, LuceneDoubleConversionUtils.toLong(2.5)));
      writer.addDocument(doubleDoc);
      writer.commit();
    }
  }

  /**
   * Wraps {@code reader} so every point traversal ({@link PointValues#getPointTree()}) increments
   * {@code traversals}. Core cache helpers are delegated so per-core caching still applies.
   */
  private static DirectoryReader countingPointTraversalReader(
      DirectoryReader reader, AtomicInteger traversals) throws IOException {
    return new FilterDirectoryReader(
        reader,
        new FilterDirectoryReader.SubReaderWrapper() {
          @Override
          public LeafReader wrap(LeafReader leaf) {
            return new FilterLeafReader(leaf) {
              @Override
              public PointValues getPointValues(String field) throws IOException {
                PointValues pointValues = super.getPointValues(field);
                return pointValues == null ? null : countingPointValues(pointValues, traversals);
              }

              @Override
              public CacheHelper getCoreCacheHelper() {
                return this.in.getCoreCacheHelper();
              }

              @Override
              public CacheHelper getReaderCacheHelper() {
                return null;
              }
            };
          }
        }) {
      @Override
      protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return countingPointTraversalReader(in, traversals);
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }

  private static PointValues countingPointValues(PointValues in, AtomicInteger traversals) {
    return new PointValues() {
      @Override
      public PointTree getPointTree() throws IOException {
        traversals.incrementAndGet();
        return in.getPointTree();
      }

      @Override
      public byte[] getMinPackedValue() throws IOException {
        return in.getMinPackedValue();
      }

      @Override
      public byte[] getMaxPackedValue() throws IOException {
        return in.getMaxPackedValue();
      }

      @Override
      public int getNumDimensions() throws IOException {
        return in.getNumDimensions();
      }

      @Override
      public int getNumIndexDimensions() throws IOException {
        return in.getNumIndexDimensions();
      }

      @Override
      public int getBytesPerDimension() throws IOException {
        return in.getBytesPerDimension();
      }

      @Override
      public long size() {
        return in.size();
      }

      @Override
      public int getDocCount() {
        return in.getDocCount();
      }
    };
  }

  /** Wraps {@code reader} so every leaf reports no core or reader cache helper. */
  private static DirectoryReader nullCacheKeyReader(DirectoryReader reader) throws IOException {
    return new FilterDirectoryReader(
        reader,
        new FilterDirectoryReader.SubReaderWrapper() {
          @Override
          public LeafReader wrap(LeafReader leaf) {
            return new FilterLeafReader(leaf) {
              @Override
              public CacheHelper getCoreCacheHelper() {
                return null;
              }

              @Override
              public CacheHelper getReaderCacheHelper() {
                return null;
              }
            };
          }
        }) {
      @Override
      protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return nullCacheKeyReader(in);
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }
}
