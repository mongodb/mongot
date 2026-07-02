package com.xgen.mongot.cursor;

import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.BatchSizeStrategySelector;
import com.xgen.mongot.cursor.batch.ConstantBatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.CountMetaBatchProducer;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.lucene.EmptySearchBatchProducer;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.trace.SpanGuard;
import com.xgen.mongot.trace.Tracing;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;

/** Creates cursors with unique incrementing id. */
class CursorFactory {

  static class CursorAndMetaResults {
    public final MongotCursor cursor;
    public final MetaResults metaResults;

    public CursorAndMetaResults(MongotCursor cursor, MetaResults metaResults) {
      this.cursor = cursor;
      this.metaResults = metaResults;
    }
  }

  static class SearchCursorAndMetaCursor {
    public final MongotCursor searchCursor;
    public final MongotCursor metaCursor;

    public SearchCursorAndMetaCursor(MongotCursor searchCursor, MongotCursor metaCursor) {
      this.searchCursor = searchCursor;
      this.metaCursor = metaCursor;
    }
  }

  private final CursorIdSupplier cursorIdSupplier;

  CursorFactory(CursorIdSupplier cursorIdSupplier) {
    this.cursorIdSupplier = cursorIdSupplier;
  }

  CursorAndMetaResults createCursor(
      String namespace,
      InitializedIndex index,
      CursorQuery cursorQuery,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IndexUnavailableException,
          IOException,
          InvalidQueryException,
          InterruptedException,
          ReaderClosedException {
    index.throwIfUnavailableForQuerying();

    // Check to see if the index is in a state that it should return an empty cursor.
    if (doesNotExist(index)) {
      return getEmptyCursor(namespace);
    }

    // Otherwise, create the real cursor and return it.
    long cursorId = this.cursorIdSupplier.nextId();

    try (SpanGuard guard =
        Tracing.simpleSpanGuard(
            "CursorFactory.createCursor",
            Attributes.of(AttributeKey.longKey("cursorId"), cursorId))) {
      BatchSizeStrategy batchSizeStrategy =
          BatchSizeStrategySelector.forQuery(cursorQuery.getQuery(), queryCursorOptions);

      if (cursorQuery instanceof CursorQuery.Search && !(index instanceof InitializedSearchIndex)) {
        throw new InvalidQueryException(
            "Cannot execute $search over vectorSearch index '%s'"
                .formatted(cursorQuery.getQuery().index()),
            InvalidQueryException.Type.STRICT);
      }
      if (cursorQuery instanceof CursorQuery.Vector
          && !(index instanceof InitializedVectorIndex)
          && !(index instanceof InitializedMaterializedViewIndex)) {
        throw new InvalidQueryException(
            "Cannot execute $vectorSearch over search index '%s'"
                .formatted(cursorQuery.getQuery().index()),
            InvalidQueryException.Type.STRICT);
      }

      BatchProducer batchProducer;
      MetaResults metaResults;
      switch (cursorQuery) {
        case CursorQuery.Search(var query) -> {
          var r =
              index
                  .asSearchIndex()
                  .getReader()
                  .query(query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
          batchProducer = r.searchBatchProducer;
          metaResults = r.metaResults;
        }
        case CursorQuery.Vector(var query, var context) -> {
          VectorIndexReader reader;
          if (index instanceof InitializedVectorIndex vectorIndex) {
            reader = vectorIndex.getReader();
          } else {
            // Guard above narrows this to InitializedMaterializedViewIndex.
            reader = ((InitializedMaterializedViewIndex) index).getReader();
          }
          var r =
              reader.query(
                  query, context, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
          batchProducer = r.vectorBatchProducer;
          metaResults = r.metaResults;
        }
      }

      return new CursorAndMetaResults(
          new MongotCursor(cursorId, batchProducer, namespace, batchSizeStrategy), metaResults);
    }
  }

  SearchCursorAndMetaCursor createIntermediateCursors(
      String namespace,
      InitializedSearchIndex index,
      SearchQuery query,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IndexUnavailableException, IOException, InvalidQueryException, InterruptedException {
    index.throwIfUnavailableForQuerying();

    // Check to see if the index is in a state that it should return an empty cursor.
    if (doesNotExist(index)) {
      return getEmptyIntermediateCursors(namespace);
    }

    // Otherwise, create the real cursors and return them.

    BatchSizeStrategy searchBatchSizeStrategy =
        BatchSizeStrategySelector.forQuery(query, queryCursorOptions);
    BatchSizeStrategy metaBatchSizeStrategy =
        BatchSizeStrategySelector.forQuery(query, queryCursorOptions);
    // Note: These two strategies start off identical, but could diverge later.

    SearchIndexReader.SearchProducerAndMetaProducer producerAndMetaProducer =
        index
            .getReader()
            .intermediateQuery(
                query, queryCursorOptions, searchBatchSizeStrategy, queryOptimizationFlags);

    long resultCursorId = this.cursorIdSupplier.nextId();

    long metaCursorId = this.cursorIdSupplier.nextId();

    try (SpanGuard guard =
        Tracing.simpleSpanGuard(
            "createIntermediateCursors",
            Attributes.of(
                AttributeKey.longKey("resultCursorId"),
                resultCursorId,
                AttributeKey.longKey("metaCursorId"),
                metaCursorId))) {
      return new SearchCursorAndMetaCursor(
          new MongotCursor(
              resultCursorId,
              producerAndMetaProducer.searchBatchProducer,
              namespace,
              searchBatchSizeStrategy),
          new MongotCursor(
              metaCursorId,
              producerAndMetaProducer.metaBatchProducer,
              namespace,
              metaBatchSizeStrategy));
    }
  }

  CursorAndMetaResults getEmptyCursor(String namespace) {
    EmptySearchBatchProducer emptyProducer = new EmptySearchBatchProducer();
    return new CursorAndMetaResults(
        new MongotCursor(
            this.cursorIdSupplier.nextId(),
            emptyProducer,
            namespace,
            new ConstantBatchSizeStrategy()),
        emptyProducer.getMetaResults());
  }

  SearchCursorAndMetaCursor getEmptyIntermediateCursors(String namespace) {
    try (SpanGuard guard = Tracing.simpleSpanGuard("emptyIntermediateCursors")) {
      return new SearchCursorAndMetaCursor(
          new MongotCursor(
              this.cursorIdSupplier.nextId(),
              new EmptySearchBatchProducer(),
              namespace,
              new ConstantBatchSizeStrategy()),
          new MongotCursor(
              this.cursorIdSupplier.nextId(),
              new CountMetaBatchProducer(0),
              namespace,
              new ConstantBatchSizeStrategy()));
    }
  }

  static boolean doesNotExist(Index index) {
    return index.getStatus().getStatusCode() == IndexStatus.StatusCode.DOES_NOT_EXIST;
  }
}
