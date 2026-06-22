package com.xgen.mongot.cursor.batch;

public interface BatchSizeStrategy {

  /**
   * On non-sharded clusters, the first batch mongod sends the client contains 101 documents (and
   * not 16MB). Unless we respond with MongotCursorResult#EXHAUSTED_CURSOR_ID, mongod will eagerly
   * query for a second batch, even if the amount of supplied documents satisfies the $limit. So the
   * minimum work we would do per query is fetch two batches. For more information see
   * https://tinyurl.com/2p8acuj9
   */
  int DEFAULT_BATCH_SIZE = 101;

  /**
   * Maximum batch size was determined based on the number of documents with only ObjectIDs that
   * would fit in 1 batch for a query. For more information see https://tinyurl.com/y5b649a8
   */
  int MAXIMUM_BATCH_SIZE = 324768;

  /**
   * Minimum batch size was set where there's no noticeable difference in query latency between 10
   * and a number less than 10. For more information see https://tinyurl.com/y5b649a8
   */
  @Deprecated(since = "MongoDB 8.1", forRemoval = true)
  int MINIMUM_BATCH_SIZE = 10;

  /**
   * Returns the selected next batch size. This method must be idempotent (repeated calls have to
   * produce the same result until {@link BatchSizeStrategy#adjust} is invoked).
   */
  int adviseNextBatchSize();

  /**
   * Selects the next batch size to optimize for the most efficient data transfer. The selected
   * value does not take into account any byte size limit and should be additionally capped when
   * needed.
   */
  void adjust(BatchCursorOptions options);
}
