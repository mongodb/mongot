package com.xgen.mongot.index.lucene.writer;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.WriterClosedException;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;

/**
 * MultiLuceneIndexWriter is an {@link IndexWriter} that indexes documents into multiple {@link
 * SingleLuceneIndexWriter}s.
 */
public class MultiLuceneIndexWriter implements LuceneIndexWriter {
  // A random Prime number to avoid uneven distribution due to hash conflicts.
  private static final int HASH_SEED = 1867391047;
  // The actual indexPartition will be decided by the following hash function.
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128(HASH_SEED);

  private final List<SingleLuceneIndexWriter> indexWriters;

  private MultiLuceneIndexWriter(List<SingleLuceneIndexWriter> indexWriters) {
    this.indexWriters = indexWriters;
  }

  public static MultiLuceneIndexWriter create(List<SingleLuceneIndexWriter> indexWriters) {
    Check.checkState(
        indexWriters.size() >= 2,
        "LuceneMultiIndexWriter must be created with 2 or more LuceneIndexWriters.");
    return new MultiLuceneIndexWriter(indexWriters);
  }

  /**
   * Computes the partition ID for a given document ID.
   *
   * <p>This method uses a Murmur3 hash function to distribute documents evenly across partitions.
   * The hash is computed over the byte representation of the document ID, and the result is
   * converted to an unsigned value to ensure uniform distribution.
   *
   * @param bytesRef the byte representation of the document ID to hash
   * @param numPartitions the total number of partitions to distribute across
   * @return the partition ID (0-based index) where the document should be placed
   */
  public static int getIndexPartitionId(BytesRef bytesRef, int numPartitions) {
    // `Integer.toUnsignedLong` is called to handle the case when the hash function returns a
    // negative integer.
    return (int)
        (Integer.toUnsignedLong(
                HASH_FUNCTION.hashBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length).asInt())
            % numPartitions);
  }

  private int getIndexPartitionId(byte[] encodedDocumentId) {
    return getIndexPartitionId(new BytesRef(encodedDocumentId), this.indexWriters.size());
  }

  @Override
  public void updateIndex(DocumentEvent event) throws IOException, FieldExceededLimitsException {
    byte[] encodedDocumentId = LuceneDocumentIdEncoder.encodeDocumentId(event.getDocumentId());
    int indexPartitionId = getIndexPartitionId(encodedDocumentId);
    this.indexWriters.get(indexPartitionId).updateIndex(encodedDocumentId, event);
  }

  @Override
  public void commit(EncodedUserData userData) throws IOException {
    // We commit the userData in the reverse order.
    // So that the first LuceneIndexWriter is the last one to be committed.
    // If a mongot crash causes only a portion of LuceneIndexWriters being commited with newer
    // userData, the first LuceneIndexWriter must be committed with older userData. The replication
    // system will attempt to index events again.
    for (SingleLuceneIndexWriter indexWriter : Lists.reverse(this.indexWriters)) {
      indexWriter.commit(userData);
    }
  }

  @Override
  public EncodedUserData getCommitUserData() {
    // Only returns the userData from the first LuceneIndexWriter. The userData from other
    // LuceneIndexWriters might be inaccurate. Refer to the comments in commit() and deleteAll() for
    // more context.
    return this.indexWriters.get(0).getCommitUserData();
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    // We check FieldExceededLimitsException before DocExceededLimitsException because
    // FieldExceededLimitsException indicates a more severe error.
    Optional<FieldExceededLimitsException> exceededFieldLimits =
        this.indexWriters.stream()
            .map(SingleLuceneIndexWriter::exceededFieldLimits)
            .flatMap(Optional::stream)
            .findAny();
    if (exceededFieldLimits.isPresent()) {
      return exceededFieldLimits.map(Function.identity());
    }
    return this.indexWriters.stream()
        .map(SingleLuceneIndexWriter::exceededDocsLimits)
        .flatMap(Optional::stream)
        .findAny()
        .map(Function.identity());
  }

  @Override
  public void deleteAll(EncodedUserData userData) throws IOException {
    // Call deleteAll in the sequential order. The first LuceneIndexWriter will be deleted first.
    // If a mongot crash causes only a portion of LuceneIndexWriters being deleted, the first
    // LuceneIndexWriter must be deleted with correct userData. The replication system will try
    // to delete index data from other LuceneIndexWriters.
    CheckedStream.fromSequential(this.indexWriters)
        .forEachChecked(indexWriter -> indexWriter.deleteAll(userData));
  }

  @Override
  public void close() throws IOException {
    CheckedStream.from(this.indexWriters).forEachChecked(IndexWriter::close);
  }

  @Override
  public int getNumFields() throws WriterClosedException {
    // Count distinct field names from all LuceneIndexWriters.
    Set<String> distinctFieldNames = new HashSet<>();
    CheckedStream.from(this.indexWriters)
        .forEachChecked(indexWriter -> distinctFieldNames.addAll(indexWriter.getFieldNames()));
    return distinctFieldNames.size();
  }

  @Override
  public long getNumDocs() throws WriterClosedException {
    return CheckedStream.from(this.indexWriters)
        .mapAndCollectChecked(IndexWriter::getNumDocs)
        .stream()
        .mapToLong(Long::longValue)
        .sum();
  }

  @Override
  public long getNumLuceneMaxDocs() throws WriterClosedException {
    return CheckedStream.from(this.indexWriters)
        .mapAndCollectChecked(SingleLuceneIndexWriter::getNumLuceneMaxDocs)
        .stream()
        .mapToLong(Long::longValue)
        .sum();
  }

  @Override
  public int getMaxLuceneMaxDocs() throws WriterClosedException {
    return CheckedStream.from(this.indexWriters)
        .mapAndCollectChecked(SingleLuceneIndexWriter::getMaxLuceneMaxDocs)
        .stream()
        .mapToInt(Integer::intValue)
        .max()
        .orElseThrow();
  }

  @Override
  public int getNumWriters() {
    return this.indexWriters.size();
  }

  @Override
  public void cancelMerges() throws IOException {
    // Cancel merges on all underlying index writers
    CheckedStream.from(this.indexWriters).forEachChecked(IndexWriter::cancelMerges);
  }

  public List<SingleLuceneIndexWriter> getSingleLuceneIndexWriters() {
    return this.indexWriters;
  }
}
