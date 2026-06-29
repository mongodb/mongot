package com.xgen.mongot.index.lucene.codec;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BIT;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BYTE;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q1;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q7;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_VECTOR;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.codec.flat.BinaryQuantizedFlatVectorsFormat;
import com.xgen.mongot.index.lucene.codec.flat.FlatBitVectorsFormat;
import com.xgen.mongot.index.lucene.codec.flat.Float32AndByteFlatVectorsFormat;
import com.xgen.mongot.index.lucene.codec.flat.ScalarQuantizedFlatVectorsFormat;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBinaryQuantizedVectorsFormat;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBitVectorsFormat;
import com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.backward_codecs.lucene99.Lucene94FieldInfosFormatV1;
import org.apache.lucene.backward_codecs.lucene99.Lucene99PostingsFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90CompoundFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90LiveDocsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90NormsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PointsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90TermVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99SegmentInfoFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * Change-detector tests that lock which sub-format {@link LuceneCodec} writes — both the per-field
 * dispatch (which format each field is routed to) and the concrete class of every sub-format slot.
 * These are intentional tripwires: a failure means mongot is about to write a different format to
 * disk than the previous release, which can break rollback (an older binary that cannot decode the
 * new bytes must re-index).
 *
 * <p>This is one of three layered guards enforcing mongot's codec rollback-safety contract, each
 * locking a different axis:
 *
 * <ul>
 *   <li>{@code LuceneForkVersionTest} — which jar each Lucene module loads from.
 *   <li>{@code CodecSubformatChangeDetectorTest} — per-field dispatch and the class of every
 *       sub-format slot.
 *   <li>{@code OnDiskFormatVersionsTest} — the on-disk version each format writes.
 * </ul>
 *
 * <p>See the codec sub-format rollout policy:
 * docs/internal/development/codec-subformat-rollout-policy.md.
 */
@RunWith(Theories.class)
public class CodecSubformatChangeDetectorTest {

  /**
   * Golden table mapping each vector {@link FieldName.TypeField} to the {@link KnnVectorsFormat}
   * the writer dispatches to under the default HNSW indexing algorithm (see {@code
   * LuceneCodec.resolveHnswVectorFormat}).
   */
  private static final ImmutableMap<FieldName.TypeField, Class<? extends KnnVectorsFormat>>
      EXPECTED_HNSW_VECTOR_FORMATS =
          ImmutableMap.<FieldName.TypeField, Class<? extends KnnVectorsFormat>>builder()
              .put(KNN_VECTOR, Lucene99HnswVectorsFormat.class)
              .put(KNN_BYTE, Lucene99NativeHnswVectorsFormat.class)
              .put(KNN_BIT, Mongot01042HnswBitVectorsFormat.class)
              .put(KNN_F32_Q7, Lucene99HnswScalarQuantizedVectorsFormat.class)
              .put(KNN_F32_Q1, Mongot01042HnswBinaryQuantizedVectorsFormat.class)
              .build();

  /**
   * Golden table mapping each vector {@link FieldName.TypeField} to the {@link KnnVectorsFormat}
   * the writer dispatches to under the flat indexing algorithm (see {@code
   * LuceneCodec.resolveFlatVectorFormat}).
   */
  private static final ImmutableMap<FieldName.TypeField, Class<? extends KnnVectorsFormat>>
      EXPECTED_FLAT_VECTOR_FORMATS =
          ImmutableMap.<FieldName.TypeField, Class<? extends KnnVectorsFormat>>builder()
              .put(KNN_VECTOR, Float32AndByteFlatVectorsFormat.class)
              .put(KNN_BYTE, Float32AndByteFlatVectorsFormat.class)
              .put(KNN_BIT, FlatBitVectorsFormat.class)
              .put(KNN_F32_Q7, ScalarQuantizedFlatVectorsFormat.class)
              .put(KNN_F32_Q1, BinaryQuantizedFlatVectorsFormat.class)
              .build();

  @DataPoints("allTypeFields")
  public static final FieldName.TypeField[] allTypeFields = FieldName.TypeField.values();

  private static final String DISPATCH_CHANGE_DETECTOR_MESSAGE =
      "LuceneCodec vector-format dispatch changed for type field '%s'. Changing which "
          + "KnnVectorsFormat a writer dispatches to -- or adding a new KNN type field -- can "
          + "break n-1 rollback: an older reader cannot decode segments written by a format it "
          + "does not register, forcing a reindex. Any such change must follow a staged, "
          + "reader-before-writer rollout. If this change is intentional, update the golden table "
          + "in this test and follow the codec sub-format rollout policy at "
          + "docs/internal/development/codec-subformat-rollout-policy.md";

  /**
   * Checks every sub-format of the default {@link LuceneCodec} against its expected implementation.
   * Breaking this test means a codec sub-format changed, which can break forward compatibility (old
   * sub-formats cannot read segments written by newer versions) and force rollbacks to re-index.
   *
   * <p>For the per-field formats (postings, docValues, knnVectors) we check both the wrapper class
   * and its registered name (written into segment metadata), as well as the underlying per-field
   * delegate class. For the remaining formats, we check the implementation class.
   */
  @Test
  public void subformatNames_defaultCodec_matchExpectedLucene99Formats() {
    LuceneCodec codec = new LuceneCodec();

    assertEquals("Lucene99", codec.getName());

    // Per-field formats: check wrapper class, registered name, and underlying delegate.
    // The registered name is written into FieldInfo attributes and must stay stable across
    // upgrades.
    assertThat(codec.postingsFormat()).isInstanceOf(PerFieldPostingsFormat.class);
    PerFieldPostingsFormat postingsFormat = (PerFieldPostingsFormat) codec.postingsFormat();
    assertEquals("PerField40", postingsFormat.getName());
    assertThat(postingsFormat.getPostingsFormatForField("any"))
        .isInstanceOf(Lucene99PostingsFormat.class);

    assertThat(codec.docValuesFormat()).isInstanceOf(PerFieldDocValuesFormat.class);
    PerFieldDocValuesFormat docValuesFormat = (PerFieldDocValuesFormat) codec.docValuesFormat();
    assertEquals("PerFieldDV40", docValuesFormat.getName());
    assertThat(docValuesFormat.getDocValuesFormatForField("any"))
        .isInstanceOf(Lucene90DocValuesFormat.class);

    // knnVectorsFormat requires a typed field name because LuceneCodec.getKnnVectorsFormatForField
    // parses the type prefix to resolve the format.
    assertThat(codec.knnVectorsFormat()).isInstanceOf(PerFieldKnnVectorsFormat.class);
    PerFieldKnnVectorsFormat knnVectorsFormat = (PerFieldKnnVectorsFormat) codec.knnVectorsFormat();
    assertEquals("PerFieldVectors90", knnVectorsFormat.getName());
    assertThat(
            knnVectorsFormat.getKnnVectorsFormatForField(
                KNN_VECTOR.getLuceneFieldName(FieldPath.parse("any"), Optional.empty())))
        .isInstanceOf(Lucene99HnswVectorsFormat.class);

    // Remaining formats delegated from Lucene99Codec (except storedFieldsFormat, which LuceneCodec
    // overrides with its own LuceneStoredFieldsFormat).
    assertThat(codec.storedFieldsFormat()).isInstanceOf(LuceneStoredFieldsFormat.class);
    assertThat(codec.fieldInfosFormat()).isInstanceOf(Lucene94FieldInfosFormatV1.class);
    assertThat(codec.segmentInfoFormat()).isInstanceOf(Lucene99SegmentInfoFormat.class);
    assertThat(codec.termVectorsFormat()).isInstanceOf(Lucene90TermVectorsFormat.class);
    assertThat(codec.liveDocsFormat()).isInstanceOf(Lucene90LiveDocsFormat.class);
    assertThat(codec.compoundFormat()).isInstanceOf(Lucene90CompoundFormat.class);
    assertThat(codec.normsFormat()).isInstanceOf(Lucene90NormsFormat.class);
    assertThat(codec.pointsFormat()).isInstanceOf(Lucene90PointsFormat.class);
  }

  /**
   * Exhaustive change-detector over every {@link FieldName.TypeField} for the writer's default HNSW
   * per-field vector dispatch. It fails whenever a dispatched format class changes, and -- because
   * it iterates every enum value and requires non-vector types to throw -- whenever a new KNN type
   * field is wired into dispatch without a golden-table entry. Both are rollback-unsafe changes
   * that must follow the codec sub-format rollout policy linked in the class Javadoc.
   */
  @Theory
  public void knnVectorsFormat_hnswDispatch_everyTypeField_matchesGoldenTable(
      @FromDataPoints("allTypeFields") FieldName.TypeField typeField) {
    LuceneCodec codec = new LuceneCodec();

    assertDispatchMatchesGoldenTable(codec, typeField, EXPECTED_HNSW_VECTOR_FORMATS);
  }

  /**
   * Exhaustive change-detector over every {@link FieldName.TypeField} for the writer's flat
   * per-field vector dispatch. See {@link
   * #knnVectorsFormat_hnswDispatch_everyTypeField_matchesGoldenTable}.
   */
  @Theory
  public void knnVectorsFormat_flatDispatch_everyTypeField_matchesGoldenTable(
      @FromDataPoints("allTypeFields") FieldName.TypeField typeField) {
    FieldPath path = FieldPath.parse("vector");
    LuceneCodec codec = new LuceneCodec(Map.of(path, flatVectorFieldDefinition()));

    assertDispatchMatchesGoldenTable(codec, typeField, EXPECTED_FLAT_VECTOR_FORMATS);
  }

  private static void assertDispatchMatchesGoldenTable(
      LuceneCodec codec,
      FieldName.TypeField typeField,
      ImmutableMap<FieldName.TypeField, Class<? extends KnnVectorsFormat>> goldenTable) {
    PerFieldKnnVectorsFormat format = (PerFieldKnnVectorsFormat) codec.knnVectorsFormat();
    String luceneField = typeField.getLuceneFieldName(FieldPath.parse("vector"), Optional.empty());

    if (goldenTable.containsKey(typeField)) {
      assertWithMessage(DISPATCH_CHANGE_DETECTOR_MESSAGE, typeField)
          .that(format.getKnnVectorsFormatForField(luceneField))
          .isInstanceOf(goldenTable.get(typeField));
      return;
    }

    // A type field with no golden entry must not resolve to a KnnVectorsFormat. This is what
    // prevents a newly added KNN type field from being wired into writer dispatch without a
    // golden-table entry (and the policy review that editing the table forces).
    KnnVectorsFormat leaked;
    try {
      leaked = format.getKnnVectorsFormatForField(luceneField);
    } catch (IllegalStateException expected) {
      return;
    }
    throw new AssertionError(
        String.format(DISPATCH_CHANGE_DETECTOR_MESSAGE, typeField)
            + " It resolved to "
            + leaked.getClass().getName()
            + " but is missing from the golden table.");
  }

  private static VectorFieldSpecification flatVectorFieldDefinition() {
    return new VectorFieldSpecification(
        1024,
        VectorSimilarity.DOT_PRODUCT,
        VectorQuantization.NONE,
        new VectorIndexingAlgorithm.FlatIndexingAlgorithm());
  }
}
