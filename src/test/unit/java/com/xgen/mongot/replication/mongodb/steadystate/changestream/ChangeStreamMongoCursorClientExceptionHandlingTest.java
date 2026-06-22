package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoCursorClient;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException.Type;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.mongodb.Errors.Error;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.Assert;
import org.junit.Test;

public class ChangeStreamMongoCursorClientExceptionHandlingTest {

  @Test
  public void searchRequestRejectedDueToOverloadCode_isInRetryableErrorCodes() {
    Assert.assertTrue(
        Errors.RETRYABLE_ERROR_CODES.contains(Errors.SEARCH_REQUEST_REJECTED_DUE_TO_OVERLOAD.code));
  }

  @Test
  public void testHandlingTransient() {
    testException(new MongoInterruptedException("", null), Type.TRANSIENT);

    testException(new MongoClientException(""), Type.TRANSIENT);

    testException(new MongoException(""), Type.TRANSIENT);

    testException(new CodecConfigurationException(""), Type.TRANSIENT);

    {
      Error error = Errors.BAD_VALUE;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.TRANSIENT);
    }

    {
      Error error = Errors.UNAUTHORIZED;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.TRANSIENT);
    }

    {
      Error error = Errors.NAMESPACE_NOT_FOUND;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.TRANSIENT);
    }

    {
      Error error = Errors.INDEX_NOT_FOUND;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.TRANSIENT);
    }

    {
      Error error = Errors.CURSOR_NOT_FOUND;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.TRANSIENT);
    }

    {
      Error error = Errors.INDEX_ALREADY_EXISTS;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.TRANSIENT);
    }

    {
      Error error = Errors.SHARD_NOT_FOUND;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.TRANSIENT);
    }

    {
      Error error = Errors.QUERY_PLAN_KILLED;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.TRANSIENT);
    }

    {
      Error error = Errors.INDEX_INFORMATION_TOO_LARGE;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.TRANSIENT);
    }

    {
      for (var errorCode : Errors.RETRYABLE_ERROR_CODES) {
        MongoException mongoClientException = new MongoException(errorCode, errorCode.toString());
        testException(mongoClientException, Type.TRANSIENT);
      }
    }
  }

  @Test
  public void testHandlingNonInvalidatingResync() {
    {
      Error error = Errors.CAPPED_POSITION_LOST;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.NON_INVALIDATING_RESYNC);
    }

    {
      Error error = Errors.CHANGE_STREAM_FATAL_ERROR;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.NON_INVALIDATING_RESYNC);
    }

    {
      Error error = Errors.CHANGE_STREAM_HISTORY_LOST;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.NON_INVALIDATING_RESYNC);
    }

    {
      Error error = Errors.BSON_OBJECT_TOO_LARGE;
      MongoException mongoClientException = new MongoException(error.code, error.name);
      testException(mongoClientException, Type.NON_INVALIDATING_RESYNC);
    }
  }

  private void testException(Throwable error, SteadyStateException.Type expectedType) {
    var cursorClient = createMockBatchCursorClient(error);

    SteadyStateException returnedException =
        Assert.assertThrows(SteadyStateException.class, cursorClient::getNext);
    Assert.assertNotNull(returnedException);
    Assert.assertEquals(expectedType, returnedException.getType());
  }

  private ChangeStreamMongoCursorClient<SteadyStateException> createMockBatchCursorClient(
      Throwable error) {
    BatchMongoClient batchMongoClient = mock(BatchMongoClient.class);
    SessionRefresher sessionRefresher = mock(SessionRefresher.class);

    com.mongodb.client.ClientSession clientSession = mock(com.mongodb.client.ClientSession.class);
    when(batchMongoClient.openSession(any())).thenReturn(clientSession);
    when(sessionRefresher.register(any())).thenThrow(error);

    return new ChangeStreamMongoCursorClient<>(
        mock(GenerationId.class),
        batchMongoClient,
        sessionRefresher,
        mock(AggregateOperationTemplate.class),
        new SimpleMeterRegistry(),
        SteadyStateException::wrapIfThrows,
        Optional.empty());
  }
}
