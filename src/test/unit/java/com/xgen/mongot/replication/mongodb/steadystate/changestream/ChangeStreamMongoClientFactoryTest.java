package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ClientSession;
import com.mongodb.internal.operation.AggregateResponseBatchCursor;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamPartition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.PartitionedChangeStreamClient;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamMongoClientFactory.ChangeStreamClientFactory;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamMongoClientFactory.TimedChangeStreamClientFactory;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;

public class ChangeStreamMongoClientFactoryTest {

  private static final GenerationId GENERATION_ID =
      new GenerationId(new ObjectId(), Generation.CURRENT);

  @Test
  public void resumeTimedModeAwareChangeStream_parallelReadersDisabled_returnsTimedClient()
      throws Exception {
    ChangeStreamMongoClientFactory factory =
        ChangeStreamMongoClientFactory.create(
            mock(SessionRefresher.class),
            mock(BatchMongoClient.class),
            mock(ChangeStreamModeSelector.class),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1)
                .setChangeStreamQueryMaxTimeMs(1000)
                .setChangeStreamCursorMaxTimeSec(1000)
                .setEnableParallelChangeStreamReaders(false)
                .build(),
            Optional.empty());

    ChangeStreamMongoClient<?> client =
        factory.resumeTimedModeAwareChangeStream(
            GENERATION_ID, resumeInfo(), SearchIndexDefinitionBuilder.VALID_INDEX, false, false);

    assertThat(client).isInstanceOf(TimedChangeStreamClient.class);
  }

  @Test
  public void resumeTimedModeAwareChangeStream_parallelReadersEnabled_returnsPartitionedClient()
      throws Exception {
    ChangeStreamMongoClientFactory factory =
        ChangeStreamMongoClientFactory.create(
            mock(SessionRefresher.class),
            mock(BatchMongoClient.class),
            mock(ChangeStreamModeSelector.class),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(1)
                .setMaxInFlightEmbeddingGetMores(1)
                .setChangeStreamQueryMaxTimeMs(1000)
                .setChangeStreamCursorMaxTimeSec(1000)
                .setEnableParallelChangeStreamReaders(true)
                .setNumParallelChangeStreamReaders(3)
                .build(),
            Optional.of(Runnable::run));

    ChangeStreamMongoClient<?> client =
        factory.resumeTimedModeAwareChangeStream(
            GENERATION_ID, resumeInfo(), SearchIndexDefinitionBuilder.VALID_INDEX, false, true);

    assertThat(client).isInstanceOf(PartitionedChangeStreamClient.class);
    client.close();
  }

  @Test
  public void resumeTimedModeAwareChangeStream_excludedIndex_remainsSingleReaderOnModeSwitch()
      throws Exception {
    var readers = mock(Executor.class);
    CursorFixture fixture = cursorFixture(readers);
    var definition =
        VectorIndexDefinitionBuilder.builder().withCosineVectorField("vector", 3).build();

    try (var client =
        fixture
            .factory()
            .resumeTimedModeAwareChangeStream(
                GENERATION_ID, resumeInfo(), definition, false, false)) {
      assertThat(client).isInstanceOf(TimedChangeStreamClient.class);
      assertThat(client.getNext().getPartition()).isEmpty();
      fixture.mode().set(ChangeStreamMode.INDEXED_FIELDS);
      assertThat(client.getNext().getPartition()).isEmpty();
      assertThat(client.getNext().getPartition()).isEmpty();
    }

    assertThat(fixture.cursors()).hasSize(2);
    verify(fixture.cursors().getFirst()).close();
    verify(fixture.cursors().getLast()).close();
    verify(readers, never()).execute(any(Runnable.class));
  }

  @Test
  public void partitionClientFactory_firstOpen_opensByOperationTime() {
    var factory = factoryWithParallelReadersEnabled(3);
    var operationFactory = mock(ChangeStreamAggregateOperationFactory.class);
    var cursorFactory = mock(ChangeStreamClientFactory.class);
    var namespace = new MongoNamespace("test", "test");
    var partition = new ChangeStreamPartition(1, 3);
    var start = new BsonTimestamp(100, 0);
    var operationTemplate = mock(AggregateOperationTemplate.class);

    when(cursorFactory.resumeChangeStream(any(), any()))
        .thenReturn(mock(ChangeStreamMongoClient.class));
    when(operationFactory.fromOperationTime(
            eq(namespace), eq(start), eq(ChangeStreamMode.getDefault()), eq(partition)))
        .thenReturn(operationTemplate);

    TimedChangeStreamClientFactory restartFactory =
        factory.partitionClientFactory(
            operationFactory, cursorFactory, namespace, start, partition);
    restartFactory.resumeChangeStream(GENERATION_ID, placeholderResumeInfo(namespace));

    verify(cursorFactory).resumeChangeStream(GENERATION_ID, operationTemplate);
    verify(operationFactory, never()).fromResumeInfo(any(), any(), any());
  }

  @Test
  public void partitionClientFactory_restart_resumesFromPartitionTokenNotOperationTime() {
    var factory = factoryWithParallelReadersEnabled(3);
    var operationFactory = mock(ChangeStreamAggregateOperationFactory.class);
    var cursorFactory = mock(ChangeStreamClientFactory.class);
    var namespace = new MongoNamespace("test", "test");
    var partition = new ChangeStreamPartition(1, 3);
    var start = new BsonTimestamp(100, 0);
    var restartToken = ChangeStreamUtils.resumeToken(new BsonTimestamp(200, 0));
    var operationTemplate = mock(AggregateOperationTemplate.class);

    when(cursorFactory.resumeChangeStream(any(), any()))
        .thenReturn(mock(ChangeStreamMongoClient.class));
    when(operationFactory.fromResumeInfo(
            eq(ChangeStreamResumeInfo.create(namespace, restartToken)),
            eq(ChangeStreamMode.getDefault()),
            eq(partition)))
        .thenReturn(operationTemplate);

    TimedChangeStreamClientFactory restartFactory =
        factory.partitionClientFactory(
            operationFactory, cursorFactory, namespace, start, partition);

    // first call: the initial open, which opens by operation time (asserted separately above)
    restartFactory.resumeChangeStream(GENERATION_ID, placeholderResumeInfo(namespace));
    // second call: simulates a TimedChangeStreamClient TTL (or mode-switch) restart
    restartFactory.resumeChangeStream(
        GENERATION_ID, ChangeStreamResumeInfo.create(namespace, restartToken));

    verify(cursorFactory).resumeChangeStream(GENERATION_ID, operationTemplate);
    // the restart must resume from the partition's own token, not re-open by operation time again
    verify(operationFactory, times(1)).fromOperationTime(any(), any(), any(), eq(partition));
  }

  @Test
  public void resumeTimedModeAwareChangeStream_vectorAutoEmbedding_keepsSingleReaderBatchSize()
      throws Exception {
    assertSingleEmbeddingReader(
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField("text").build());
  }

  @Test
  public void resumeTimedModeAwareChangeStream_searchAutoEmbedding_keepsSingleReaderBatchSize()
      throws Exception {
    assertSingleEmbeddingReader(
        SearchIndex.mockAutoEmbeddingSearchDefinition(GENERATION_ID.indexId));
  }

  @Test
  public void resumeTimedModeAwareChangeStream_legacyText_keepsSingleReaderBatchSize()
      throws Exception {
    assertSingleEmbeddingReader(
        VectorIndexDefinitionBuilder.builder().withTextField("text").build());
  }

  private static void assertSingleEmbeddingReader(IndexDefinition definition) throws Exception {
    var readers = mock(Executor.class);
    var fixture = cursorFixture(readers);

    try (var client =
        fixture
            .factory()
            .resumeTimedModeAwareChangeStream(
                GENERATION_ID, resumeInfo(), definition, false, false)) {
      assertThat(client).isInstanceOf(TimedChangeStreamClient.class);
      assertThat(client.getNext().getPartition()).isEmpty();
      fixture.mode().set(ChangeStreamMode.INDEXED_FIELDS);
      assertThat(client.getNext().getPartition()).isEmpty();
      assertThat(client.getNext().getPartition()).isEmpty();
    }

    assertThat(fixture.cursors()).hasSize(2);
    fixture.cursors().forEach(cursor -> verify(cursor).setBatchSize(17));
    verify(fixture.cursors().getFirst()).close();
    verify(fixture.cursors().getLast()).close();
    verify(readers, never()).execute(any(Runnable.class));
  }

  @Test
  public void resumeTimedModeAwareChangeStream_searchIndex_retainsDefaultGetMoreBatchSize()
      throws Exception {
    var fixture = cursorFixture(Runnable::run);

    try (var client =
        fixture
            .factory()
            .resumeTimedModeAwareChangeStream(
                GENERATION_ID,
                resumeInfo(),
                SearchIndexDefinitionBuilder.VALID_INDEX,
                false,
                true)) {
      client.getNext();

      assertThat(fixture.cursors()).hasSize(3);
      fixture.cursors().forEach(cursor -> verify(cursor, never()).setBatchSize(anyInt()));
    }
  }

  private static CursorFixture cursorFixture(Executor readerExecutor) {
    var mongoClient = mock(BatchMongoClient.class);
    when(mongoClient.getSettings()).thenReturn(MongoClientSettings.builder().build());
    when(mongoClient.openSession(any())).thenAnswer(unused -> mock(ClientSession.class));
    var cursors = new ArrayList<AggregateResponseBatchCursor<RawBsonDocument>>();
    when(mongoClient.openCursor(any(), any(), any()))
        .thenAnswer(
            unused -> {
              @SuppressWarnings("unchecked")
              AggregateResponseBatchCursor<RawBsonDocument> cursor =
                  mock(AggregateResponseBatchCursor.class);
              when(cursor.getPostBatchResumeToken())
                  .thenReturn(ChangeStreamUtils.resumeToken(new BsonTimestamp(2, 0)));
              when(cursor.tryNext()).thenReturn(List.of());
              cursors.add(cursor);
              return cursor;
            });
    var mode = new AtomicReference<>(ChangeStreamMode.ALL_FIELDS);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    when(modeSelector.getMode(any())).thenAnswer(unused -> mode.get());
    var factory =
        ChangeStreamMongoClientFactory.create(
            com.xgen.testing.mongot.mock.replication.mongodb.common.SessionRefresher
                .mockSessionRefresher(),
            mongoClient,
            modeSelector,
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            SteadyStateReplicationConfig.builder()
                .setNumConcurrentChangeStreams(3)
                .setMaxInFlightEmbeddingGetMores(1)
                .setChangeStreamQueryMaxTimeMs(1000)
                .setChangeStreamCursorMaxTimeSec(1000)
                .setEnableParallelChangeStreamReaders(true)
                .setNumParallelChangeStreamReaders(3)
                .setEmbeddingGetMoreBatchSize(Optional.of(17))
                .setEnableSplitLargeChangeStreamEvents(true)
                .build(),
            Optional.of(readerExecutor));
    return new CursorFixture(factory, mode, cursors);
  }

  private record CursorFixture(
      ChangeStreamMongoClientFactory factory,
      AtomicReference<ChangeStreamMode> mode,
      List<AggregateResponseBatchCursor<RawBsonDocument>> cursors) {}

  private ChangeStreamMongoClientFactory factoryWithParallelReadersEnabled(int partitions) {
    var modeSelector = mock(ChangeStreamModeSelector.class);
    when(modeSelector.getMode(any())).thenReturn(ChangeStreamMode.getDefault());
    return ChangeStreamMongoClientFactory.create(
        mock(SessionRefresher.class),
        mock(BatchMongoClient.class),
        modeSelector,
        MeterAndFtdcRegistry.createWithSimpleRegistries(),
        SteadyStateReplicationConfig.builder()
            .setNumConcurrentChangeStreams(1)
            .setMaxInFlightEmbeddingGetMores(1)
            .setChangeStreamQueryMaxTimeMs(1000)
            .setChangeStreamCursorMaxTimeSec(1000)
            .setEnableParallelChangeStreamReaders(true)
            .setNumParallelChangeStreamReaders(partitions)
            .build(),
        Optional.of(Runnable::run));
  }

  private static ChangeStreamResumeInfo placeholderResumeInfo(MongoNamespace namespace) {
    return ChangeStreamResumeInfo.create(namespace, new BsonDocument());
  }

  private static ChangeStreamResumeInfo resumeInfo() {
    return ChangeStreamResumeInfo.create(
        new MongoNamespace("test", "test"), ChangeStreamUtils.resumeToken(new BsonTimestamp(1, 0)));
  }
}
