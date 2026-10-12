package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamPartition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamMongoClientFactory.ChangeStreamClientFactory;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class SyncModeAwareChangeStreamClientTest {

  @Test
  public void testModeAwareClientProxiesRequestAndResponseAsIs() throws Exception {
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            mock(ChangeStreamModeSelector.class),
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            new GenerationId(new ObjectId(), Generation.CURRENT));

    var batch = mock(ChangeStreamBatch.class);
    when(syncClient.getNext()).thenReturn(batch);

    // make sure mode aware client proxies the request and returns response from the wrapped client
    Assert.assertEquals(batch, modeAwareClient.getNext());
  }

  @Test
  public void testModeAwareClientProxiesExceptionFromWrappedClient() throws Exception {
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            mock(ChangeStreamModeSelector.class),
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            new GenerationId(new ObjectId(), Generation.CURRENT));

    // emulate wrapped client getMore() failure
    when(syncClient.getNext()).thenThrow(new RuntimeException("getMore() request failure"));

    // make sure the Future has failed
    Exception ex = Assert.assertThrows(Exception.class, modeAwareClient::getNext);
    Assert.assertEquals("getMore() request failure", ex.getMessage());
  }

  @Test
  public void testModeAwareClientFailureOnWrappedClientRestart() throws Exception {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);
    when(syncClient.getNext()).thenReturn(mock(ChangeStreamBatch.class));
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.ALL_FIELDS);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            modeSelector,
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            generationId);

    verify(clientFactory).resumeChangeStream(eq(generationId), any());

    // another mode selection should trigger client restart
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.INDEXED_FIELDS);

    // emulate client restart failure
    when(clientFactory.resumeChangeStream(any(), any()))
        .thenThrow(new RuntimeException("restart failure test"));

    // make sure the Future has failed
    Exception ex = Assert.assertThrows(Exception.class, modeAwareClient::getNext);
    Assert.assertEquals("restart failure test", ex.getMessage());
  }

  @Test
  public void testModeIsSwitchedIfInitialSelectionDoesNotMatchSubsequentSelection()
      throws Exception {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    var operationFactory = mock(ChangeStreamAggregateOperationFactory.class);
    var indexedOperationTemplate = mock(AggregateOperationTemplate.class);
    when(operationFactory.fromResumeInfo(any(), eq(ChangeStreamMode.INDEXED_FIELDS)))
        .thenReturn(indexedOperationTemplate);
    var allFieldsOperationTemplate = mock(AggregateOperationTemplate.class);
    when(operationFactory.fromResumeInfo(any(), eq(ChangeStreamMode.ALL_FIELDS)))
        .thenReturn(allFieldsOperationTemplate);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    // mode is set at the beginning
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.INDEXED_FIELDS);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            modeSelector,
            operationFactory,
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            generationId);

    verify(clientFactory).resumeChangeStream(eq(generationId), eq(indexedOperationTemplate));

    // mode has changed after client creation
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.ALL_FIELDS);

    when(syncClient.getNext()).thenReturn(mock(ChangeStreamBatch.class));
    modeAwareClient.getNext();

    // verify client has restarted
    verify(clientFactory).resumeChangeStream(eq(generationId), eq(allFieldsOperationTemplate));

    // verify the mode is not flipped on subsequent getMore() calls
    modeAwareClient.getNext();
    verifyNoMoreInteractions(clientFactory);
  }

  @Test
  public void testModeIsNotSwitchedIfInitialSelectionMatchSubsequentSelections() throws Exception {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    // INDEXED_FIELDS mode is selected initially
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.INDEXED_FIELDS);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            modeSelector,
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            generationId);

    verify(clientFactory).resumeChangeStream(eq(generationId), any());

    // the same mode is selected before next getMore() call
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.INDEXED_FIELDS);

    when(syncClient.getNext()).thenReturn(mock(ChangeStreamBatch.class));
    modeAwareClient.getNext();

    // make sure client is not restarted
    verifyNoMoreInteractions(clientFactory);
  }

  @Test
  public void testDefaultClientIsClosed() {
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);
    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            mock(ChangeStreamModeSelector.class),
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            new GenerationId(new ObjectId(), Generation.CURRENT));

    modeAwareClient.close();
    verify(syncClient).close();
  }

  @Test
  public void partitionAwareConstructor_initialConstruction_buildsFromOperationTime()
      throws Exception {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);
    var operationFactory = mock(ChangeStreamAggregateOperationFactory.class);
    var operationTemplate = mock(AggregateOperationTemplate.class);
    var partition = new ChangeStreamPartition(1, 3);
    var startAtOperationTime = new BsonTimestamp(100, 0);
    var namespace = new MongoNamespace("test", "test");

    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.ALL_FIELDS);
    when(operationFactory.fromOperationTime(
            namespace, startAtOperationTime, ChangeStreamMode.ALL_FIELDS, partition))
        .thenReturn(operationTemplate);
    when(clientFactory.resumeChangeStream(generationId, operationTemplate)).thenReturn(syncClient);

    new ModeAwareChangeStreamClient(
        modeSelector,
        operationFactory,
        clientFactory,
        namespace,
        startAtOperationTime,
        partition,
        generationId);

    verify(clientFactory).resumeChangeStream(generationId, operationTemplate);
  }

  @Test
  public void partitionAwareConstructor_ttlRestart_buildsFromResumeInfoWithPartition()
      throws Exception {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);
    var operationFactory = mock(ChangeStreamAggregateOperationFactory.class);
    var partition = new ChangeStreamPartition(1, 3);
    var namespace = new MongoNamespace("test", "test");

    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.ALL_FIELDS);
    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    // partition-aware "restart" constructor, as used by TimedChangeStreamClient on TTL restart.
    new ModeAwareChangeStreamClient(
        modeSelector,
        operationFactory,
        clientFactory,
        ChangeStreamResumeInfo.create(namespace, getResumeToken("restart")),
        partition,
        generationId);

    verify(operationFactory)
        .fromResumeInfo(
            eq(ChangeStreamResumeInfo.create(namespace, getResumeToken("restart"))),
            eq(ChangeStreamMode.ALL_FIELDS),
            eq(partition));
  }

  @Test
  public void partitionAwareClient_modeSwitch_resumesFromPartitionToken() throws Exception {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);
    var operationFactory = mock(ChangeStreamAggregateOperationFactory.class);
    var partition = new ChangeStreamPartition(2, 3);
    var namespace = new MongoNamespace("test", "test");
    var startAtOperationTime = new BsonTimestamp(100, 0);

    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.INDEXED_FIELDS);
    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            modeSelector,
            operationFactory,
            clientFactory,
            namespace,
            startAtOperationTime,
            partition,
            generationId);

    var batch = mock(ChangeStreamBatch.class);
    when(batch.getPostBatchResumeToken()).thenReturn(getResumeToken("afterFirstBatch"));
    when(syncClient.getNext()).thenReturn(batch);

    // mode switches after the client is constructed
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.ALL_FIELDS);
    modeAwareClient.getNext();

    // restart used the partition's own post-batch resume token, with its partition filter
    verify(operationFactory)
        .fromResumeInfo(
            eq(ChangeStreamResumeInfo.create(namespace, getResumeToken("afterFirstBatch"))),
            eq(ChangeStreamMode.ALL_FIELDS),
            eq(partition));
  }

  @Test
  public void partitionAwareClient_close_closesWrappedClientOnly() {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);
    var operationFactory = mock(ChangeStreamAggregateOperationFactory.class);
    var partition = new ChangeStreamPartition(0, 3);
    var namespace = new MongoNamespace("test", "test");

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            modeSelector,
            operationFactory,
            clientFactory,
            namespace,
            new BsonTimestamp(100, 0),
            partition,
            generationId);

    modeAwareClient.close();

    // closes the wrapped client; the one recorded construction is the initial one above
    verify(syncClient).close();
    verify(clientFactory).resumeChangeStream(any(), any());
  }

  private BsonDocument getResumeToken(String token) {
    return new BsonDocument("token", new BsonString(token));
  }
}
