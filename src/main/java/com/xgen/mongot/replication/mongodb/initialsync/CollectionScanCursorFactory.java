package com.xgen.mongot.replication.mongodb.initialsync;

import com.mongodb.ReadConcern;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.MetadataNamespace;
import com.xgen.mongot.replication.mongodb.common.Projection;
import com.xgen.mongot.replication.mongodb.common.ViewPipeline;
import com.xgen.mongot.util.mongodb.CollectionScanAggregateCommand;
import java.util.function.Consumer;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

/**
 * Opens the cursor a collection scan reads from. Shared by the single-cursor and parallel scanners
 * so that both turn one index definition into the same pipeline.
 */
final class CollectionScanCursorFactory {

  static final BsonDocument NATURAL_ASC = new BsonDocument("$natural", new BsonInt32(1));

  private CollectionScanCursorFactory() {}

  static CollectionScanMongoClient<InitialSyncException> open(
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      Consumer<CollectionScanAggregateCommand.Builder> scanOrder)
      throws InitialSyncException {
    // note that the aggregate command does not support noCursorTimeout option, but
    // it's not required as 1) we run it in scope of a session 2) cursors that belong to a session
    // are never ripped 3) we refresh sessions in the background thread to avoid their timeout
    // after 30 minutes of inactivity. See SERVER-15042 and SERVER-6036 for details
    var builder =
        new CollectionScanAggregateCommand.Builder(
                context.getIndexDefinition().getLastObservedCollectionName())
            .metadataAddFieldsStage(
                MetadataNamespace.forRegularQuery(context.getIndexDefinition().getIndexId()))
            .readConcern(ReadConcern.MAJORITY, context.getChangeStreamResumeOperationTime());

    scanOrder.accept(builder);

    context
        .getIndexDefinition()
        .getView()
        .ifPresent(view -> builder.viewDefinedStages(ViewPipeline.forRegularQuery(view)));

    Projection.forRegularQuery(context.getIndexDefinition())
        .ifPresent(builder::indexedFieldsProjectionStage);

    context.getEmbeddingGetMoreBatchSize().ifPresent(builder::batchSize);

    return mongoClient.getCollectionAggregateCommandMongoClient(
        builder.build(),
        context.getIndexDefinition(),
        context.getInitialSyncMetricsUpdater(),
        context.getEmbeddingGetMoreBatchSize());
  }
}
