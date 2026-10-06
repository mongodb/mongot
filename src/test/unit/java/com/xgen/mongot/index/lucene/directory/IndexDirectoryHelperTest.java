package com.xgen.mongot.index.lucene.directory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

public class IndexDirectoryHelperTest {
  private static final Path ROOT_PATH = Path.of(System.getenv("TEST_TMPDIR"));

  private MetricsFactory metricsFactory;
  private IndexDirectoryHelper helper;
  private Path directory;

  @Before
  public void setUp() throws IOException {
    this.metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    this.helper =
        new IndexDirectoryHelper(
            ROOT_PATH,
            this.metricsFactory,
            new AtomicDirectoryRemover(Files.createTempDirectory("trash")));
    this.directory = Files.createTempDirectory("index");
  }

  private boolean recover(Exception cause, boolean isIndexDropped, boolean isRetainOnDiskEnabled)
      throws IOException {
    return this.helper.attemptToRecoverUnreadableIndex(
        new GenerationId(new ObjectId(), Generation.FIRST),
        this.directory,
        cause,
        isIndexDropped,
        isRetainOnDiskEnabled);
  }

  private double recoveryCount(String cause) {
    return this.metricsFactory
        .get("unreadableIndexRecoveries", Tags.of(Tag.of("unreadableIndexCause", cause)))
        .counter()
        .count();
  }

  private boolean directoryExists() {
    return this.directory.toFile().exists();
  }

  /**
   * Triggers a real Lucene SPI lookup failure by asking {@link KnnVectorsFormat} to resolve a name
   * that does not exist on the classpath -- the same {@code NamedSPILoader} path a downgraded
   * binary hits when opening a segment written with a newer format. We source the exception from
   * Lucene (rather than hand-writing the message) so that if a Lucene upgrade changes the wording,
   * this test fails instead of the SPI matcher in {@link
   * IndexDirectoryHelper#attemptToRecoverUnreadableIndex} silently no longer matching.
   */
  private static IllegalArgumentException realLuceneSpiLookupFailure() {
    IllegalArgumentException spiError =
        assertThrows(
            IllegalArgumentException.class,
            () -> KnnVectorsFormat.forName("MongotNonexistentFormatForSpiMatcherTest"));
    assertTrue(spiError.getMessage().contains("does not exist"));
    return spiError;
  }

  @Test
  public void attemptToRecoverUnreadableIndex_droppedIndex_recoversAndIncrementsDroppedCounter()
      throws IOException {
    // A dropped index recovers regardless of the underlying exception.
    boolean recovered = recover(new IOException("index no longer exists"), true, false);

    assertTrue(recovered);
    assertEquals(1.0, this.metricsFactory.get("unreadableDroppedIndexes").counter().count(), 0);
    assertFalse(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_corruptIndex_recoversAndIncrementsCorruptCounter()
      throws IOException {
    boolean recovered = recover(new CorruptIndexException("corrupt", "resource"), false, false);

    assertTrue(recovered);
    assertEquals(1.0, recoveryCount("corruptIndex"), 0);
    assertFalse(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_noSuchFile_recoversAndIncrementsCorruptCounter()
      throws IOException {
    boolean recovered = recover(new NoSuchFileException("segments_1"), false, false);

    assertTrue(recovered);
    assertEquals(1.0, recoveryCount("corruptIndex"), 0);
    assertFalse(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_indexFormatTooOld_recoversAndIncrementsFormatCounter()
      throws IOException {
    boolean recovered =
        recover(new IndexFormatTooOldException("resource", "too old"), false, false);

    assertTrue(recovered);
    assertEquals(1.0, recoveryCount("incompatibleIndexFormat"), 0);
    assertFalse(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_indexFormatTooNew_recoversAndIncrementsFormatCounter()
      throws IOException {
    boolean recovered = recover(new IndexFormatTooNewException("resource", 5, 1, 4), false, false);

    assertTrue(recovered);
    assertEquals(1.0, recoveryCount("incompatibleIndexFormat"), 0);
    assertFalse(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_couldNotLoadCodec_recoversAndIncrementsCodecCounter()
      throws IOException {
    boolean recovered =
        recover(new IllegalArgumentException("Could not load codec 'Lucene9999'"), false, false);

    assertTrue(recovered);
    assertEquals(1.0, recoveryCount("incompatibleCodec"), 0);
    assertFalse(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_tooOldWithCodecMessage_prefersCodecCounter()
      throws IOException {
    // The case that motivated checking the codec message ahead of the too-old/too-new branch:
    // Lucene's SegmentInfos#readCodec wraps an unknown "Lucene*" codec name in an
    // IndexFormatTooOldException whose message begins "Could not load codec" (rather than a plain
    // IllegalArgumentException). Such an exception matches BOTH the codec branch (by message) and
    // the too-old branch (by type), so ordering decides attribution -- the codec counter must win.
    // A plain-IAE codec test cannot catch a regression here because it never matches the too-old
    // branch. (The full write-then-read path is exercised in InitializedLuceneSearchIndexTest;
    // this asserts the precedence directly at the decision site.)
    boolean recovered =
        recover(
            new IndexFormatTooOldException(
                "resource",
                "Could not load codec 'Lucene9999'. An SPI class of type"
                    + " org.apache.lucene.codecs.Codec with name 'Lucene9999' does not exist. Did"
                    + " you forget to add lucene-backward-codecs.jar?"),
            false,
            false);

    assertTrue(recovered);
    assertEquals(1.0, recoveryCount("incompatibleCodec"), 0);
    assertEquals(0.0, recoveryCount("incompatibleIndexFormat"), 0);
    assertFalse(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_spiFormatNotFound_recoversAndIncrementsSpiCounter()
      throws IOException {
    // Source the exception from real Lucene so this test guards the SPI matcher in
    // IndexDirectoryHelper against Lucene message drift across upgrades.
    boolean recovered = recover(realLuceneSpiLookupFailure(), false, false);

    assertTrue(recovered);
    assertEquals(1.0, recoveryCount("incompatibleSpiFormat"), 0);
    assertFalse(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_unrelatedIllegalArgument_doesNotRecover()
      throws IOException {
    boolean recovered =
        recover(
            new IllegalArgumentException("some unrelated argument does not exist"), false, false);

    assertFalse(recovered);
    assertEquals(0.0, recoveryCount("incompatibleSpiFormat"), 0);
    assertEquals(0.0, recoveryCount("incompatibleCodec"), 0);
    // Unrecoverable: the directory is left untouched.
    assertTrue(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_unrecognizedException_doesNotRecover()
      throws IOException {
    boolean recovered = recover(new IOException("transient disk error"), false, false);

    assertFalse(recovered);
    assertTrue(directoryExists());
  }

  // Retain-failed-data-on-disk behavior (counter still increments so alerts/downgrade tests fire,
  // but the directory is preserved and recovery is not attempted -> returns false) lives in the
  // shared handleIndexError, which every recovery branch routes through. We assert it for each
  // branch so that a future refactor bypassing handleIndexError for one branch is caught here.
  private void assertRetainOnDiskKeepsDirectory(Exception cause, String counterTag)
      throws IOException {
    boolean recovered = recover(cause, false, true);

    assertFalse(recovered);
    assertEquals(1.0, recoveryCount(counterTag), 0);
    assertTrue(directoryExists());
  }

  @Test
  public void attemptToRecoverUnreadableIndex_retainOnDiskEnabled_corruptIndex_keepsDir()
      throws IOException {
    assertRetainOnDiskKeepsDirectory(
        new CorruptIndexException("corrupt", "resource"), "corruptIndex");
  }

  @Test
  public void attemptToRecoverUnreadableIndex_retainOnDiskEnabled_indexFormatTooOld_keepsDir()
      throws IOException {
    assertRetainOnDiskKeepsDirectory(
        new IndexFormatTooOldException("resource", "too old"), "incompatibleIndexFormat");
  }

  @Test
  public void attemptToRecoverUnreadableIndex_retainOnDiskEnabled_couldNotLoadCodec_keepsDir()
      throws IOException {
    assertRetainOnDiskKeepsDirectory(
        new IllegalArgumentException("Could not load codec 'Lucene9999'"), "incompatibleCodec");
  }

  @Test
  public void attemptToRecoverUnreadableIndex_retainOnDiskEnabled_spiFormatNotFound_keepsDir()
      throws IOException {
    assertRetainOnDiskKeepsDirectory(realLuceneSpiLookupFailure(), "incompatibleSpiFormat");
  }

  // ---------------------------------------------------------------------------
  // Path construction and definition-free deletion.
  //
  // These paths are derived from a GenerationId alone, with no parsed index definition, which is
  // what lets a quarantined (unparseable) definition still have its data cleaned up.
  // ---------------------------------------------------------------------------

  private static GenerationId generationId(ObjectId indexId, int formatVersion, int userVersion) {
    return new GenerationId(
        indexId,
        new Generation(
            new UserIndexVersion(userVersion), IndexFormatVersion.create(formatVersion)));
  }

  /**
   * A helper whose trash directory is a sibling under ROOT_PATH. AtomicDirectoryRemover moves a
   * directory into trash before deleting it, which requires both to be on the same volume, so the
   * deletion tests below cannot rely on a trash directory in the system temp dir.
   */
  private IndexDirectoryHelper helperWithTrashUnderRoot() throws IOException {
    Path trash = Files.createTempDirectory(ROOT_PATH, "trash");
    return new IndexDirectoryHelper(
        ROOT_PATH, this.metricsFactory, new AtomicDirectoryRemover(trash));
  }

  private static void createWithFile(Path directory) throws IOException {
    Files.createDirectories(directory);
    // A non-empty directory, so a delete that only unlinks empty dirs would not pass.
    Files.writeString(directory.resolve("segments_1"), "data");
  }

  @Test
  public void pathAndNameOverloads_definitionAndGenerationId_agree() {
    // The IndexDefinitionGeneration overloads exist only to spare callers that already hold a
    // definition; they must be pure delegation, or an index's data would move depending on which
    // overload a caller happened to pick.
    ObjectId indexId = new ObjectId();
    VectorIndexDefinitionGeneration definition =
        new VectorIndexDefinitionGeneration(
            VectorIndex.mockVectorDefinition(indexId), Generation.CURRENT);
    GenerationId generationId = definition.getGenerationId();

    assertEquals(
        this.helper.getIndexDirectoryPath(generationId),
        this.helper.getIndexDirectoryPath(definition));
    assertEquals(
        this.helper.getIndexMetadataPath(generationId),
        this.helper.getIndexMetadataPath(definition));
    assertEquals(
        IndexDirectoryHelper.getNameForGeneration(generationId),
        IndexDirectoryHelper.getNameForGeneration(definition));
  }

  @Test
  public void getIndexDirectoryPath_firstGeneration_usesBareIndexId() {
    // Indexes predating generations were stored under a bare indexId; that layout must be
    // reproducible from identity alone or their data becomes unreachable, and undeletable.
    ObjectId indexId = new ObjectId();
    GenerationId generationId = new GenerationId(indexId, Generation.FIRST);

    assertEquals(
        ROOT_PATH.resolve(indexId.toHexString()), this.helper.getIndexDirectoryPath(generationId));
  }

  @Test
  public void getIndexDirectoryPath_laterGeneration_usesGenerationEncodedName() {
    ObjectId indexId = new ObjectId();
    GenerationId generationId = generationId(indexId, 6, 3);

    assertEquals(
        ROOT_PATH.resolve(indexId.toHexString() + "_f6_u3_a0"),
        this.helper.getIndexDirectoryPath(generationId));
  }

  @Test
  public void getIndexMetadataPath_firstGeneration_usesGenerationEncodedName() {
    // Deliberately asymmetric with getIndexDirectoryPath: metadata has no legacy bare-indexId
    // layout, so even Generation.FIRST resolves under the generation-encoded name.
    ObjectId indexId = new ObjectId();
    GenerationId generationId = new GenerationId(indexId, Generation.FIRST);

    assertEquals(
        ROOT_PATH.resolve("indexMetadata").resolve(indexId.toHexString() + "_f5_u0_a0"),
        this.helper.getIndexMetadataPath(generationId));
  }

  @Test
  public void getNameForGeneration_isNotInterchangeableWithUniqueString() {
    // GenerationId.uniqueString() is the hyphenated form. Substituting one for the other would
    // silently relocate every index directory on disk.
    GenerationId generationId = generationId(new ObjectId(), 6, 3);

    assertEquals(
        generationId.indexId.toHexString() + "_f6_u3_a0",
        IndexDirectoryHelper.getNameForGeneration(generationId));
    assertNotEquals(
        IndexDirectoryHelper.getNameForGeneration(generationId), generationId.uniqueString());
  }

  @Test
  public void isGenerationDirectoryName_tracksGeneratedNameFormat() {
    GenerationId generationId = generationId(new ObjectId(), 6, 3);

    assertTrue(
        IndexDirectoryHelper.isGenerationDirectoryName(
            IndexDirectoryHelper.getNameForGeneration(generationId)));
    assertFalse(IndexDirectoryHelper.isGenerationDirectoryName(generationId.indexId.toHexString()));
    assertFalse(IndexDirectoryHelper.isGenerationDirectoryName(generationId.uniqueString()));
  }

  @Test
  public void getIndexDirectoryPath_unsupportedFormatVersion_throws() {
    GenerationId generationId = generationId(new ObjectId(), 4, 0);

    assertThrows(
        IllegalStateException.class, () -> this.helper.getIndexDirectoryPath(generationId));
  }

  @Test
  public void deleteGenerationData_unsupportedFormatVersion_throws() {
    // Deliberately preserves the pre-existing behavior of getIndexDirectoryPath rather than
    // degrading to a no-op: the definition-free delete path introduces no new tolerance for
    // format versions mongot does not support.
    GenerationId generationId = generationId(new ObjectId(), 4, 0);

    assertThrows(IllegalStateException.class, () -> this.helper.deleteGenerationData(generationId));
  }

  @Test
  public void deleteGenerationData_removesBothDataAndMetadata() throws IOException {
    IndexDirectoryHelper helperUnderRoot = helperWithTrashUnderRoot();
    GenerationId generationId = generationId(new ObjectId(), 6, 1);
    Path dataPath = helperUnderRoot.getIndexDirectoryPath(generationId);
    Path metadataPath = helperUnderRoot.getIndexMetadataPath(generationId);
    createWithFile(dataPath);
    createWithFile(metadataPath);

    helperUnderRoot.deleteGenerationData(generationId);

    assertFalse(Files.exists(dataPath));
    assertFalse(Files.exists(metadataPath));
  }

  @Test
  public void deleteGenerationData_firstGeneration_removesLegacyDataAndMetadata()
      throws IOException {
    // The legacy layout is the case where a path bug would delete the wrong directory, since the
    // data path is a bare indexId shared by no other generation naming scheme.
    IndexDirectoryHelper helperUnderRoot = helperWithTrashUnderRoot();
    GenerationId generationId = new GenerationId(new ObjectId(), Generation.FIRST);
    Path dataPath = helperUnderRoot.getIndexDirectoryPath(generationId);
    Path metadataPath = helperUnderRoot.getIndexMetadataPath(generationId);
    createWithFile(dataPath);
    createWithFile(metadataPath);

    helperUnderRoot.deleteGenerationData(generationId);

    assertFalse(Files.exists(dataPath));
    assertFalse(Files.exists(metadataPath));
  }

  @Test
  public void deleteGenerationData_missingDirectories_isNoOp() throws IOException {
    // Must be idempotent: a quarantined entry may be re-processed after a crash that interrupted
    // an earlier delete, and startup cannot fail because the data is already gone.
    IndexDirectoryHelper helperUnderRoot = helperWithTrashUnderRoot();
    GenerationId generationId = generationId(new ObjectId(), 6, 1);

    helperUnderRoot.deleteGenerationData(generationId);
    helperUnderRoot.deleteGenerationData(generationId);

    assertFalse(Files.exists(helperUnderRoot.getIndexDirectoryPath(generationId)));
  }

  @Test
  public void deleteGenerationData_leavesOtherGenerationsOfSameIndexIntact() throws IOException {
    IndexDirectoryHelper helperUnderRoot = helperWithTrashUnderRoot();
    ObjectId indexId = new ObjectId();
    GenerationId doomed = generationId(indexId, 6, 1);
    GenerationId survivor = generationId(indexId, 6, 2);
    GenerationId legacySurvivor = new GenerationId(indexId, Generation.FIRST);
    createWithFile(helperUnderRoot.getIndexDirectoryPath(doomed));
    createWithFile(helperUnderRoot.getIndexMetadataPath(doomed));
    createWithFile(helperUnderRoot.getIndexDirectoryPath(survivor));
    createWithFile(helperUnderRoot.getIndexMetadataPath(survivor));
    createWithFile(helperUnderRoot.getIndexDirectoryPath(legacySurvivor));
    createWithFile(helperUnderRoot.getIndexMetadataPath(legacySurvivor));

    helperUnderRoot.deleteGenerationData(doomed);

    assertFalse(Files.exists(helperUnderRoot.getIndexDirectoryPath(doomed)));
    assertFalse(Files.exists(helperUnderRoot.getIndexMetadataPath(doomed)));
    assertTrue(Files.exists(helperUnderRoot.getIndexDirectoryPath(survivor)));
    assertTrue(Files.exists(helperUnderRoot.getIndexMetadataPath(survivor)));
    // The bare-indexId directory of the same index must survive a generation-encoded delete.
    assertTrue(Files.exists(helperUnderRoot.getIndexDirectoryPath(legacySurvivor)));
    assertTrue(Files.exists(helperUnderRoot.getIndexMetadataPath(legacySurvivor)));
  }
}
