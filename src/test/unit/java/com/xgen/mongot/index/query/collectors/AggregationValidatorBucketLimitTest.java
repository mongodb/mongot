package com.xgen.mongot.index.query.collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.collectors.AggregationValidator.AggregationValidationException;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Test;

/** Tests for {@link AggregationValidator#validateBucketLimit}. */
public class AggregationValidatorBucketLimitTest {

  private static final String REGION_FIELD = "region";
  private static final String SKU_FIELD = "sku";
  private static final String REGION_LUCENE_FIELD =
      TypeField.TOKEN.getLuceneFieldName(FieldPath.parse(REGION_FIELD), Optional.empty());
  private static final String SKU_LUCENE_FIELD =
      TypeField.TOKEN.getLuceneFieldName(FieldPath.parse(SKU_FIELD), Optional.empty());

  private Directory directory;
  private IndexReader reader;

  @After
  public void tearDown() throws IOException {
    if (this.reader != null) {
      this.reader.close();
    }
    if (this.directory != null) {
      this.directory.close();
    }
  }

  /**
   * One document per (region, sku) pair, {@code docsPerSegment} documents per segment, so both the
   * segment count and the per-segment value counts are deterministic.
   */
  private void buildIndex(int numRegions, int numSkus, int docsPerSegment) throws IOException {
    this.directory = new ByteBuffersDirectory();
    try (IndexWriter writer =
        new IndexWriter(
            this.directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      @Var int docsInSegment = 0;
      for (int region = 0; region < numRegions; region++) {
        for (int sku = 0; sku < numSkus; sku++) {
          Document doc = new Document();
          doc.add(
              new SortedSetDocValuesField(REGION_LUCENE_FIELD, new BytesRef("region-" + region)));
          doc.add(new SortedSetDocValuesField(SKU_LUCENE_FIELD, new BytesRef("sku-" + sku)));
          writer.addDocument(doc);
          docsInSegment++;
          if (docsInSegment == docsPerSegment) {
            writer.commit();
            docsInSegment = 0;
          }
        }
      }
    }
    this.reader = DirectoryReader.open(this.directory);
  }

  private static AggregationCollector collector(AggregationFacetDefinition... facets) {
    Map<String, AggregationFacetDefinition> byName = new LinkedHashMap<>();
    for (AggregationFacetDefinition facet : facets) {
      byName.put("by" + facet.path(), facet);
    }
    return AggregationCollector.create(AllDocumentsOperator.INSTANCE, byName);
  }

  private static AggregationFacetDefinition stringFacet(
      String path, AggregationFacetDefinition... children) {
    Map<String, AggregationFacetDefinition> byName = new LinkedHashMap<>();
    for (AggregationFacetDefinition child : children) {
      byName.put("by" + child.path(), child);
    }
    return AggregationFacetDefinition.create(
        new FacetDefinition.StringFacetDefinition(path, 10), Map.of(), byName);
  }

  @Test
  public void validateBucketLimit_underTheLimit_passes() throws Exception {
    buildIndex(/* numRegions= */ 3, /* numSkus= */ 1, /* docsPerSegment= */ 100);
    AggregationCollector collector = collector(stringFacet(REGION_FIELD));

    // Single segment: region cardinality is exactly 3.
    AggregationValidator.validateBucketLimit(collector, this.reader, 3);
  }

  @Test
  public void validateBucketLimit_rootStringAboveTheLimit_throws() throws Exception {
    buildIndex(/* numRegions= */ 10, /* numSkus= */ 1, /* docsPerSegment= */ 100);
    AggregationCollector collector = collector(stringFacet(REGION_FIELD));

    AggregationValidationException e =
        assertThrows(
            AggregationValidationException.class,
            () -> AggregationValidator.validateBucketLimit(collector, this.reader, 9));

    assertThat(e).hasMessageThat().contains("10 buckets");
    assertThat(e).hasMessageThat().contains("maximum of 9");
  }

  /**
   * Per-segment counts double-count values shared across segments; the estimate takes half the
   * cross-segment sum, floored at the largest single segment's count.
   */
  @Test
  public void validateBucketLimit_cardinalityEstimateDampsSegmentDoubleCounting() throws Exception {
    // Documents are written region-major, so with 6 regions x 3 skus in segments of 3 docs,
    // every segment holds all 3 skus: sku sum = 18, largest segment = 3,
    // estimate = max(3, 18 / 2) = 9.
    buildIndex(/* numRegions= */ 6, /* numSkus= */ 3, /* docsPerSegment= */ 3);
    assertThat(this.reader.leaves()).hasSize(6);
    AggregationCollector collector = collector(stringFacet(SKU_FIELD));

    assertThrows(
        AggregationValidationException.class,
        () -> AggregationValidator.validateBucketLimit(collector, this.reader, 8));
    AggregationValidator.validateBucketLimit(collector, this.reader, 9);
  }

  /** The estimate never dips below the largest single segment's count, a true lower bound. */
  @Test
  public void validateBucketLimit_cardinalityEstimateFlooredAtLargestSegment() throws Exception {
    // Single segment of 3 regions: sum = 3, sum / 2 = 1, but the segment provably holds 3.
    buildIndex(/* numRegions= */ 3, /* numSkus= */ 1, /* docsPerSegment= */ 100);
    AggregationCollector collector = collector(stringFacet(REGION_FIELD));

    assertThrows(
        AggregationValidationException.class,
        () -> AggregationValidator.validateBucketLimit(collector, this.reader, 2));
    AggregationValidator.validateBucketLimit(collector, this.reader, 3);
  }

  /**
   * Nested levels multiply the full cardinality bound — deliberately not capped by document count,
   * since a multi-valued field fans one document into a bucket per value.
   */
  @Test
  public void validateBucketLimit_nestedStringMultipliesFullCardinality() throws Exception {
    // 25 docs in 5 segments of one region each. Region: sum 5, largest 1, estimate 2. Sku: sum
    // 25, largest 5, estimate 12.
    buildIndex(/* numRegions= */ 5, /* numSkus= */ 5, /* docsPerSegment= */ 5);
    AggregationCollector collector = collector(stringFacet(REGION_FIELD, stringFacet(SKU_FIELD)));

    // Root level: 2. Nested level: 2 * 12 = 24 — above maxDoc-style reasoning on purpose.
    // Total: 26.
    assertThrows(
        AggregationValidationException.class,
        () -> AggregationValidator.validateBucketLimit(collector, this.reader, 25));
    AggregationValidator.validateBucketLimit(collector, this.reader, 26);
  }

  @Test
  public void validateBucketLimit_absentFieldHasZeroCardinality_passes() throws Exception {
    buildIndex(/* numRegions= */ 3, /* numSkus= */ 1, /* docsPerSegment= */ 100);
    AggregationCollector collector = collector(stringFacet("missing"));

    AggregationValidator.validateBucketLimit(collector, this.reader, 0);
  }
}
