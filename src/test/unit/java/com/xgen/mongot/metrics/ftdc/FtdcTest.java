package com.xgen.mongot.metrics.ftdc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import com.xgen.mongot.util.Bytes;
import com.xgen.testing.TestUtils;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FtdcTest {
  private static final int MAX_NUM_ARCHIVE_SAMPLES = 50;
  private static final int MAX_NUM_INTERIM_SAMPLES = 3;
  private static final int MAX_NUM_FILES = 5;

  private static final long TIME = 42;

  private TemporaryFolder dir;

  @Before
  public void setUp() throws IOException {
    this.dir = TestUtils.getTempFolder();
  }

  @Test
  public void testWritesSamplesToInterim() throws Exception {
    var f = getFtdc();
    samples(f, 0, 2);
    assertInterimFileDoesNotExist();
    // we write an interim file every 3 samples
    samples(f, 2, 5);
    assertInterimFileExists();

    MetricChunk expected = chunkForSamples(0, 3);
    assertMetricChunksWritten(List.of(expected), FtdcFileType.INTERIM);
  }

  @Test
  public void testWritesSamplesOnSchemaChanged() throws Exception {
    var f = getFtdc();
    samples(f, 3, 6);

    // schema changing:
    f.addSample(new BsonDocument("bar", new BsonInt32(1)), TIME);

    // should have flushed "foo" metric
    MetricChunk chunk = chunkForSamples(3, 6);
    assertMetricChunksWritten(List.of(chunk), FtdcFileType.ARCHIVE);
    assertInterimFileDoesNotExist();
  }

  @Test
  public void testLongOverflowInDeltaEncodingWorksAsExpected() throws Exception {
    var f = getFtdc();
    BsonDocument firstDocument = new BsonDocument("foo", new BsonInt64(Long.MIN_VALUE));
    f.addSample(firstDocument, TIME);
    f.addSample(new BsonDocument("foo", new BsonInt64(Long.MAX_VALUE)), TIME);

    // the delta between (MAX_VALUE - MIN_VALUE) overflows to -1. But we will overflow when we
    // decode the chunk too (because MIN_VALUE + (-1) == MAX_VALUE) so we should be consistent.

    // change schema to force a flush:
    f.addSample(new BsonDocument("bar", new BsonInt32(1)), TIME);

    // should have flushed "foo" metric
    LinkedHashMap<String, List<Long>> samples = new LinkedHashMap<>();
    samples.put("foo", List.of(Long.MIN_VALUE, Long.MAX_VALUE));
    MetricChunk chunk = new MetricChunk(firstDocument, samples, TIME);
    assertMetricChunksWritten(List.of(chunk), FtdcFileType.ARCHIVE);
  }

  @Test
  public void testReachArchiveLimitWritesMetricToArchive() throws Exception {
    var f = getFtdc();
    samples(f, 0, 60);

    // Should have flushed after 50 samples
    MetricChunk chunk = chunkForSamples(0, MAX_NUM_ARCHIVE_SAMPLES);
    assertMetricChunksWritten(List.of(chunk), FtdcFileType.ARCHIVE);
  }

  @Test
  public void testWritesMultipleMetricsSamplesToInterim() throws Exception {
    var f = getFtdc();
    assertInterimFileDoesNotExist();
    var schema =
        new BsonDocument().append("foo", new BsonInt32(0)).append("bar", new BsonInt32(10));
    var doc1 = new BsonDocument().append("foo", new BsonInt32(1)).append("bar", new BsonInt32(20));
    var doc2 = new BsonDocument().append("foo", new BsonInt32(2)).append("bar", new BsonInt32(15));
    f.addSample(schema, TIME);
    f.addSample(doc1, 100);
    f.addSample(doc2, 200);

    assertInterimFileExists();

    LinkedHashMap<String, List<Long>> expectedSamples = new LinkedHashMap<>();
    expectedSamples.put("foo", List.of(0L, 1L, 2L));
    expectedSamples.put("bar", List.of(10L, 20L, 15L));
    MetricChunk expected = new MetricChunk(schema, expectedSamples, TIME);
    assertMetricChunksWritten(List.of(expected), FtdcFileType.INTERIM);
  }

  @Test
  public void testWritesMultipleMetricsSamplesToArchive() throws Exception {
    var f = getFtdc();
    var docs =
        IntStream.range(0, MAX_NUM_ARCHIVE_SAMPLES)
            .mapToObj(
                ignored ->
                    new BsonDocument()
                        .append("foo", new BsonInt32(1))
                        .append("bar", new BsonInt32(10)))
            .collect(Collectors.toList());
    for (BsonDocument doc : docs) {
      f.addSample(doc, TIME);
    }

    var chunks =
        FtdcDecoder.decodeChunks(
            FtdcDecoder.readDocs(this.dir.getRoot().toPath(), FtdcFileType.ARCHIVE));
    Assert.assertEquals(1, chunks.size());

    LinkedHashMap<String, List<Long>> expectedSamples = new LinkedHashMap<>();
    expectedSamples.put("foo", Collections.nCopies(MAX_NUM_ARCHIVE_SAMPLES, 1L));
    expectedSamples.put("bar", Collections.nCopies(MAX_NUM_ARCHIVE_SAMPLES, 10L));
    MetricChunk expected = new MetricChunk(docs.get(0), expectedSamples, TIME);
    assertMetricChunksWritten(List.of(expected), FtdcFileType.ARCHIVE);
  }

  @Test
  public void testArchiveWritesAreAppended() throws Exception {
    var f = getFtdc();
    samples(f, 0, 106);

    // Every 50 samples are flushed, so we would have 2 of those
    MetricChunk chunk1 = chunkForSamples(0, 50);
    MetricChunk chunk2 = chunkForSamples(50, 100);
    assertMetricChunksWritten(List.of(chunk1, chunk2), FtdcFileType.ARCHIVE);

    // there should also be a interim flush between 100 to 106 because 6 % 3 == 0
    // This chunk should start from 100 - the last archive flush:
    assertMetricChunksWritten(List.of(chunkForSamples(100, 106)), FtdcFileType.INTERIM);
  }

  @Test
  public void testSamplesAreClearedOnExceptionDueToInterimFlush() throws Exception {
    var cfg = getConfig();
    FtdcFileManager fileManager = mock(FtdcFileManager.class);
    FtdcCollector ftdcCollector = spy(new FtdcCollector());
    var f = new Ftdc(cfg, fileManager, ftdcCollector);

    f.addSample(new BsonDocument("foo", new BsonInt32(1)), 42);

    doThrow(new IOException("boom")).when(fileManager).writeAndClearInterim(any());

    clearInvocations(ftdcCollector);
    // we cause an archive flush due to a schema change
    Assert.assertThrows(
        IOException.class, () -> f.addSample(new BsonDocument("changed", new BsonInt32(1)), 42));

    // make sure we cleaned the ftdcCollector due to the exception.
    verify(ftdcCollector).clear();
  }

  @Test
  public void testSamplesAreClearedOnExceptionDueToArchiveFlush() throws Exception {
    var cfg = getConfig();
    FtdcFileManager fileManager = mock(FtdcFileManager.class);
    FtdcCollector ftdcCollector = spy(new FtdcCollector());
    var f = new Ftdc(cfg, fileManager, ftdcCollector);

    // add a couple samples
    when(ftdcCollector.getNumSamples()).thenReturn(MAX_NUM_ARCHIVE_SAMPLES);

    doThrow(new IOException("boom")).when(fileManager).writeAndClearInterim(any());

    clearInvocations(ftdcCollector);
    // this will cause an archive flush:
    Assert.assertThrows(IOException.class, () -> samples(f, 0, 1));
    // make sure we cleaned the ftdcCollector due to the exception.
    verify(ftdcCollector, atLeastOnce()).clear();
  }

  private void assertMetricChunksWritten(List<MetricChunk> expected, FtdcFileType files)
      throws Exception {
    List<BsonDocument> allDocs = FtdcDecoder.readDocs(this.dir.getRoot().toPath(), files);
    List<MetricChunk> actual = FtdcDecoder.decodeChunks(allDocs);
    Assert.assertEquals(expected, actual);
  }

  private void assertInterimFileExists() {
    var interim = interimFilePath();
    Assert.assertTrue(interim.toFile().exists());
  }

  private Path interimFilePath() {
    return this.dir.getRoot().toPath().resolve("metrics.interim");
  }

  private void assertInterimFileDoesNotExist() {
    var interim = interimFilePath();
    Assert.assertFalse(interim.toFile().exists());
  }

  private MetricChunk chunkForSamples(int start, int stop) {
    var samples = LongStream.range(start, stop).boxed().collect(Collectors.toList());
    return new MetricChunk(
        new BsonDocument("foo", new BsonInt32(start)),
        Maps.newLinkedHashMap(Map.of("foo", samples)),
        TIME);
  }

  private void samples(Ftdc f, int start, int stop) throws Exception {
    List<BsonDocument> docs = getDocs(start, stop);

    for (BsonDocument doc : docs) {
      f.addSample(doc, TIME);
    }
  }

  private List<BsonDocument> getDocs(int start, int stop) {
    return IntStream.range(start, stop)
        .mapToObj(i -> new BsonDocument("foo", new BsonInt32(i)))
        .collect(Collectors.toList());
  }

  private Ftdc getFtdc() throws IOException {
    FtdcConfig config = getConfig();
    return Ftdc.initialize(config, new FtdcMetadata.Builder().build());
  }

  private FtdcConfig getConfig() {
    return new FtdcConfig(
        this.dir.getRoot().toPath(),
        Bytes.ofKibi(10),
        Bytes.ofBytes(1000),
        MAX_NUM_INTERIM_SAMPLES,
        MAX_NUM_ARCHIVE_SAMPLES,
        MAX_NUM_FILES,
        Integer.MAX_VALUE);
  }
}
