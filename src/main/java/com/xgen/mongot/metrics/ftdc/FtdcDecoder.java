package com.xgen.mongot.metrics.ftdc;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.bson.BsonBinaryReader;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.codecs.DecoderContext;

public class FtdcDecoder {

  /** Returns FTDC file paths under {@code ftdcDir} that match the requested file type. */
  public static List<Path> getFilePaths(Path ftdcDir, FtdcFileType fileTypes) {
    File root = ftdcDir.toFile();

    Predicate<String> fileFilter =
        switch (fileTypes) {
          case INTERIM -> fileName -> fileName.startsWith("metrics.interim");
          case ARCHIVE -> fileName -> !fileName.startsWith("metrics.interim");
          case ALL -> fileName -> true;
        };

    return Arrays.stream(Optional.ofNullable(root.list()).orElse(new String[0]))
        .filter(fileFilter)
        .sorted()
        .map(fileName -> root.toPath().resolve(fileName))
        .collect(Collectors.toList());
  }

  /** Reads all FTDC documents from files matching the requested type under {@code ftdcDir}. */
  public static List<BsonDocument> readDocs(Path ftdcDir, FtdcFileType fileTypes) throws Exception {
    return CheckedStream.from(getFilePaths(ftdcDir, fileTypes))
        .mapAndCollectChecked(FtdcDecoder::readDocsFromFile)
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public static List<MetricChunk> decodeChunks(List<BsonDocument> docs) throws Exception {
    var chunks = new ArrayList<MetricChunk>();
    for (BsonDocument document : docs) {
      if (document.getInt32("type").getValue() == DocumentType.METRIC_CHUNK) {
        chunks.add(decodeMetricChunk(document));
      }
    }
    return chunks;
  }

  static MetricChunk decodeMetricChunk(BsonDocument chunkDoc) throws Exception {
    Check.checkState(
        chunkDoc.getInt32("type").getValue() == 1, "type must be 1 for a metric chunk");
    BsonDateTime time = chunkDoc.getDateTime("_id");

    // uncompress the data:
    var data = chunkDoc.getBinary("data");
    var dataBuffer = ByteBuffer.wrap(data.getData()).order(ByteOrder.LITTLE_ENDIAN);
    var uncompressedLength = dataBuffer.getInt();
    var rest = new byte[data.getData().length - 4];
    dataBuffer.get(rest);
    var uncompressed = ZlibUtil.zlibDecompress(rest);
    Check.checkState(uncompressed.length == uncompressedLength, "uncompressed length mismatch");

    // start reading the data, first the schema document
    var metricChunkBuffer = ByteBuffer.wrap(uncompressed).order(ByteOrder.LITTLE_ENDIAN);
    var schemaDoc = readOneDocument(metricChunkBuffer);
    int metricCount = metricChunkBuffer.getInt();
    int deltaCount = metricChunkBuffer.getInt();
    // deltas is a 1d array containing the deltas for all the metrics
    var deltas = decodeZeroRuns(LongPacker.unpack(metricChunkBuffer));
    checkState(deltas.size() == metricCount * deltaCount, "dimensions not adding up");

    LinkedHashMap<String, Long> initialSample = FtdcCollector.extractMetrics(schemaDoc);
    LinkedHashMap<String, List<Long>> samples = new LinkedHashMap<>();
    @Var int offset = 0;
    for (Map.Entry<String, Long> entry : initialSample.entrySet()) {
      var firstSample = entry.getValue();
      List<Long> samplesForMetric = deltaDecode(firstSample, deltas, offset, deltaCount);
      samples.put(entry.getKey(), samplesForMetric);
      offset += deltaCount;
    }

    return new MetricChunk(schemaDoc, samples, time.getValue());
  }

  static List<BsonDocument> readDocsWithoutMetadata(Path ftdcDir, FtdcFileType fileTypes)
      throws Exception {
    // check that 'type' is a number in order to not filter out documents that contain a type key
    Predicate<BsonDocument> isMetadata =
        doc -> doc.isInt32("type") && doc.getInt32("type").equals(DocumentType.METADATA_TYPE);

    return readDocs(ftdcDir, fileTypes).stream()
        .filter(isMetadata.negate())
        .collect(Collectors.toList());
  }

  public static List<BsonDocument> readDocsFromFile(Path filePath) throws IOException {
    var docsInFile = new ArrayList<BsonDocument>();
    var bytes = Files.readAllBytes(filePath);
    var buf = ByteBuffer.wrap(bytes);
    while (buf.hasRemaining()) {
      BsonDocument document = readOneDocument(buf);
      docsInFile.add(document);
    }
    return docsInFile;
  }

  static BsonDocument readOneDocument(byte[] bytes) {
    return readOneDocument(ByteBuffer.wrap(bytes));
  }

  private static BsonDocument readOneDocument(ByteBuffer buf) {
    return BsonUtils.BSON_DOCUMENT_CODEC.decode(
        new BsonBinaryReader(buf), DecoderContext.builder().build());
  }

  private static List<Long> deltaDecode(
      Long first, List<Long> allNumbers, int offset, int deltaCount) {
    ArrayList<Long> samples = Lists.newArrayList(first);
    @Var long current = first;
    for (int i = offset; i < offset + deltaCount; i++) {
      current += allNumbers.get(i);
      samples.add(current);
    }
    return samples;
  }

  private static List<Long> decodeZeroRuns(List<Long> numbers) {
    ArrayList<Long> decoded = new ArrayList<>();
    @Var boolean zeroRun = false;
    for (Long number : numbers) {
      if (zeroRun) {
        // we are in a zero run, so this number is the zero count (not including the previous one
        for (long i = 0; i < number; i++) {
          decoded.add(0L);
        }

        zeroRun = false;
        continue;
      }

      decoded.add(number);

      if (number == 0) {
        zeroRun = true;
      }
    }
    return decoded;
  }
}
