package com.xgen.mongot.index.lucene.codec;

import static com.google.common.truth.Truth.assertWithMessage;

import com.xgen.mongot.index.lucene.quantization.Mongot01042BinaryQuantizedFlatVectorsFormat;
import java.lang.reflect.Field;
import org.apache.lucene.backward_codecs.lucene99.Lucene94FieldInfosFormatV1;
import org.apache.lucene.backward_codecs.lucene99.Lucene99PostingsFormat;
import org.apache.lucene.backward_codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90CompoundFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90LiveDocsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90NormsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PointsFormat;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingTermVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99SegmentInfoFormat;
import org.apache.lucene.index.SegmentInfos;
import org.junit.Test;

/**
 * Pins the on-disk header version constant for every Lucene format mongot writes through (plus
 * mongot's own custom formats), so that any change to what mongot writes to disk is an explicit,
 * reviewed decision.
 *
 * <p>This guards two scenarios:
 *
 * <ol>
 *   <li><b>Unintended format changes.</b> Lucene upgrades and routine fork re-syncs should leave
 *       this file untouched. A failure here means something changed about the on-disk format that
 *       probably shouldn't have — most likely an upstream {@code VERSION_CURRENT} bump leaked
 *       through the fork. Revert it in the fork before merging; otherwise customers on the previous
 *       mongot release will not be able to roll back without re-indexing.
 *   <li><b>Intended format changes.</b> When mongot does need to bump an on-disk format, the
 *       failure here forces the change to be an informed decision: update the expected value in the
 *       failing test method in the same PR, and add a release-notes entry documenting that
 *       customers on the previous mongot release cannot roll back without re-indexing the affected
 *       indexes.
 * </ol>
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
public class OnDiskFormatVersionsTest {

  // ---------------------------------------------------------------------------------------------
  // Per-field codec formats (Lucene 9.0 family).
  // ---------------------------------------------------------------------------------------------

  // Lucene90PointsFormat.VERSION_CURRENT matches upstream 10.4 (v1). The 9.11-compatible write
  // path opts DOWN explicitly: the fork's backward_codecs Lucene99Codec passes VERSION_START to
  // its points writer. Written-byte identity is enforced end-to-end by
  // LuceneCodecWrittenFormatVersionsTest.
  @Test
  public void lucene90PointsFormat_versionCurrent() {
    assertVersionPin(Lucene90PointsFormat.class, "VERSION_CURRENT", 1);
  }

  @Test
  public void lucene90DocValuesFormat_versionCurrent() {
    assertVersionPin(Lucene90DocValuesFormat.class, "VERSION_CURRENT", 0);
  }

  @Test
  public void lucene90NormsFormat_versionCurrent() {
    assertVersionPin(Lucene90NormsFormat.class, "VERSION_CURRENT", 0);
  }

  @Test
  public void lucene90LiveDocsFormat_versionCurrent() {
    assertVersionPin(Lucene90LiveDocsFormat.class, "VERSION_CURRENT", 0);
  }

  @Test
  public void lucene90CompoundFormat_versionCurrent() {
    assertVersionPin(Lucene90CompoundFormat.class, "VERSION_CURRENT", 0);
  }

  // ---------------------------------------------------------------------------------------------
  // Stored fields + term vectors: VERSION_CURRENT lives on the compressing writer (which actually
  // emits bytes), not on the *Format class.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void lucene90CompressingStoredFieldsWriter_versionCurrent() {
    assertVersionPin(Lucene90CompressingStoredFieldsWriter.class, "VERSION_CURRENT", 1);
  }

  @Test
  public void lucene90CompressingTermVectorsWriter_versionCurrent() {
    assertVersionPin(Lucene90CompressingTermVectorsWriter.class, "VERSION_CURRENT", 0);
  }

  // ---------------------------------------------------------------------------------------------
  // Segment-level formats. SegmentInfos.VERSION_CURRENT is the segments_N file version
  // (VERSION_86); Lucene99SegmentInfoFormat.VERSION_CURRENT is the .si file version.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void segmentInfos_versionCurrent() {
    assertVersionPin(SegmentInfos.class, "VERSION_CURRENT", 10);
  }

  @Test
  public void lucene99SegmentInfoFormat_versionCurrent() {
    assertVersionPin(Lucene99SegmentInfoFormat.class, "VERSION_CURRENT", 0);
  }

  // ---------------------------------------------------------------------------------------------
  // Postings — the fork keeps the 9.11 PostingsFormat alive in backward_codecs for mongot to use.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void lucene99PostingsFormat_versionCurrent() {
    assertVersionPin(Lucene99PostingsFormat.class, "VERSION_CURRENT", 0);
  }

  // ---------------------------------------------------------------------------------------------
  // FieldInfos — mongot uses the fork's V1 shim that emits FORMAT_PARENT_FIELD (= 1, the 9.11
  // header value) rather than the v2 FORMAT_DOCVALUE_SKIPPER (= 2) that upstream defaults to.
  // Pin both: FORMAT_CURRENT to catch upstream-style bumps to the class, and FORMAT_PARENT_FIELD
  // because the writer literally writes that constant to disk.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void lucene94FieldInfosFormatV1_formatCurrent() {
    assertVersionPin(Lucene94FieldInfosFormatV1.class, "FORMAT_CURRENT", 2);
  }

  @Test
  public void lucene94FieldInfosFormatV1_formatParentField() {
    assertVersionPin(Lucene94FieldInfosFormatV1.class, "FORMAT_PARENT_FIELD", 1);
  }

  // ---------------------------------------------------------------------------------------------
  // Vectors — HNSW (core) + flat (core) + scalar quantized (backward_codecs in 10.x).
  // Lucene99HnswScalarQuantizedVectorsFormat composes the HNSW + flat-SQ formats and has no
  // VERSION_CURRENT of its own; its on-disk shape is covered by the entries below.
  // ---------------------------------------------------------------------------------------------

  // Lucene99HnswVectorsFormat.VERSION_CURRENT matches upstream 10.4 (v1, group-varint). The
  // 9.11-compatible write path opts DOWN explicitly: LuceneCodec passes the WriteFormat's graph
  // write version to every HNSW format, and the fork's backward_codecs writers pass
  // VERSION_START. Written-byte identity is enforced end-to-end by
  // LuceneCodecWrittenFormatVersionsTest.
  @Test
  public void lucene99HnswVectorsFormat_versionCurrent() {
    assertVersionPin(Lucene99HnswVectorsFormat.class, "VERSION_CURRENT", 1);
  }

  @Test
  public void lucene99FlatVectorsFormat_versionCurrent() {
    assertVersionPin(Lucene99FlatVectorsFormat.class, "VERSION_CURRENT", 0);
  }

  @Test
  public void lucene99ScalarQuantizedVectorsFormat_versionCurrent() {
    assertVersionPin(Lucene99ScalarQuantizedVectorsFormat.class, "VERSION_CURRENT", 1);
  }

  // ---------------------------------------------------------------------------------------------
  // mongot's custom formats. Lucene99NativeHnswVectorsFormat has no version constant of its own:
  // its writer version comes from HnswWriteParams (fork Lucene99HnswVectorsFormat constants,
  // pinned above); the bytes it writes are locked by LuceneCodecWrittenFormatVersionsTest.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void mongot01042BinaryQuantizedFlatVectorsFormat_versionCurrent() {
    assertVersionPin(Mongot01042BinaryQuantizedFlatVectorsFormat.class, "VERSION_CURRENT", 1);
  }

  /**
   * Asserts that the named static int field on {@code formatClass} equals {@code expected}.
   *
   * <p>Reads the field reflectively for two reasons:
   *
   * <ul>
   *   <li>Most of these constants are package-private and cannot be referenced from this package by
   *       name (the {@code Lucene90*Format} classes, for instance, live in {@code
   *       org.apache.lucene.codecs.lucene90}).
   *   <li>javac inlines references to {@code public static final int} constants at compile time,
   *       which can in pathological incremental-build scenarios mask a fork re-publish that didn't
   *       trip Bazel's recompilation. Reading the field reflectively forces a runtime load.
   * </ul>
   *
   * <p>Any failure to read the field — class missing, field missing, type changed, module-system
   * access denied, etc. — surfaces as a test failure ({@code AssertionError}) rather than a test
   * error: such a disappearance is itself a signal the codec moved, which is exactly what this test
   * exists to detect.
   */
  private static void assertVersionPin(Class<?> formatClass, String fieldName, int expected) {
    int actual;
    try {
      Field f = formatClass.getDeclaredField(fieldName);
      f.setAccessible(true);
      actual = f.getInt(null);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      // Catch broadly so any reflective failure lands on the same actionable failure message:
      // checked NoSuchField/IllegalAccess, InaccessibleObjectException from setAccessible under
      // the module system, IllegalArgumentException from getInt if the field type changed, and
      // LinkageError if the class can't be linked all funnel here.
      throw new AssertionError(
          "Could not read "
              + formatClass.getName()
              + "#"
              + fieldName
              + " — the field may have moved, been renamed, or had its type changed upstream. "
              + "See class javadoc.",
          e);
    }
    assertWithMessage(
            "%s#%s has changed (expected %s, found %s). This is a rollback-compat-affecting "
                + "change. Either revert the bump in the lucene-mongot fork (and re-publish the "
                + "artifact), or — if the bump is intentional — update the expected value in this "
                + "test in the same PR and add a release note that rollback to the prior mongot "
                + "release requires re-indexing every affected index. See class javadoc.",
            formatClass.getName(), fieldName, expected, actual)
        .that(actual)
        .isEqualTo(expected);
  }
}
