package com.xgen.mongot.server.grpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ManageSearchIndexUnaryCallHandlerTest {

  private interface RawBsonDocumentStreamObserver extends StreamObserver<RawBsonDocument> {}

  private static RawBsonDocument toRaw(BsonDocument doc) {
    return new RawBsonDocument(doc, new BsonDocumentCodec());
  }

  private static ManageSearchIndexUnaryCallHandler createHandler(
      CommandRegistry commandRegistry, BulkheadCommandExecutor commandExecutor) {
    HealthManager healthManager = mock(HealthManager.class);
    when(healthManager.isHealthy()).thenReturn(true);
    return new ManageSearchIndexUnaryCallHandler(commandRegistry, commandExecutor, healthManager);
  }

  @Test
  public void invoke_knownCommand_returnsResponse() {
    CommandRegistry commandRegistry = CommandRegistry.create(new SimpleMeterRegistry());
    BulkheadCommandExecutor commandExecutor =
        new BulkheadCommandExecutor(new SimpleMeterRegistry());
    try {
      commandRegistry.registerCommand(
          "manageSearchIndex",
          (CommandFactory)
              args ->
                  new Command() {
                    @Override
                    public String name() {
                      return "manageSearchIndex";
                    }

                    @Override
                    public BsonDocument run() {
                      return args;
                    }

                    @Override
                    public ExecutionPolicy getExecutionPolicy() {
                      return ExecutionPolicy.SYNC;
                    }
                  },
          true);
      ManageSearchIndexUnaryCallHandler handler = createHandler(commandRegistry, commandExecutor);
      StreamObserver<RawBsonDocument> responseObserver = mock(RawBsonDocumentStreamObserver.class);
      RawBsonDocument request = toRaw(new BsonDocument("manageSearchIndex", new BsonInt32(1)));

      handler.invoke(request, responseObserver);

      ArgumentCaptor<RawBsonDocument> captor = ArgumentCaptor.forClass(RawBsonDocument.class);
      verify(responseObserver, timeout(5000)).onNext(captor.capture());
      verify(responseObserver, timeout(5000)).onCompleted();
      verify(responseObserver, never()).onError(any());
      Assert.assertEquals(1, captor.getValue().getInt32("manageSearchIndex").getValue());
    } finally {
      commandExecutor.close();
    }
  }

  @Test
  public void invoke_unregisteredCommand_returnsBsonError() {
    CommandRegistry commandRegistry = CommandRegistry.create(new SimpleMeterRegistry());
    BulkheadCommandExecutor commandExecutor =
        new BulkheadCommandExecutor(new SimpleMeterRegistry());
    try {
      ManageSearchIndexUnaryCallHandler handler = createHandler(commandRegistry, commandExecutor);
      StreamObserver<RawBsonDocument> responseObserver = mock(RawBsonDocumentStreamObserver.class);
      RawBsonDocument request = toRaw(new BsonDocument("unknownCommand", BsonBoolean.TRUE));

      handler.invoke(request, responseObserver);

      ArgumentCaptor<RawBsonDocument> captor = ArgumentCaptor.forClass(RawBsonDocument.class);
      verify(responseObserver, timeout(5000)).onNext(captor.capture());
      verify(responseObserver, timeout(5000)).onCompleted();
      verify(responseObserver, never()).onError(any());
      Assert.assertEquals(0, captor.getValue().getInt32("ok").getValue());
      Assert.assertEquals(
          "no command registered for unknownCommand",
          captor.getValue().getString("errmsg").getValue());
    } finally {
      commandExecutor.close();
    }
  }

  @Test
  public void invoke_commandThrows_returnsBsonError() {
    CommandRegistry commandRegistry = CommandRegistry.create(new SimpleMeterRegistry());
    BulkheadCommandExecutor commandExecutor =
        new BulkheadCommandExecutor(new SimpleMeterRegistry());
    try {
      commandRegistry.registerCommand(
          "manageSearchIndex",
          (CommandFactory)
              args ->
                  new Command() {
                    @Override
                    public String name() {
                      return "manageSearchIndex";
                    }

                    @Override
                    public BsonDocument run() {
                      throw new RuntimeException("boom");
                    }

                    @Override
                    public ExecutionPolicy getExecutionPolicy() {
                      return ExecutionPolicy.SYNC;
                    }
                  },
          true);
      ManageSearchIndexUnaryCallHandler handler = createHandler(commandRegistry, commandExecutor);
      StreamObserver<RawBsonDocument> responseObserver = mock(RawBsonDocumentStreamObserver.class);
      RawBsonDocument request = toRaw(new BsonDocument("manageSearchIndex", BsonBoolean.TRUE));

      handler.invoke(request, responseObserver);

      ArgumentCaptor<RawBsonDocument> captor = ArgumentCaptor.forClass(RawBsonDocument.class);
      verify(responseObserver, timeout(5000)).onNext(captor.capture());
      verify(responseObserver, timeout(5000)).onCompleted();
      verify(responseObserver, never()).onError(any());
      Assert.assertEquals(0, captor.getValue().getInt32("ok").getValue());
      Assert.assertEquals("boom", captor.getValue().getString("errmsg").getValue());
    } finally {
      commandExecutor.close();
    }
  }

  @Test
  public void invoke_unhealthyServer_returnsUnavailable() {
    CommandRegistry commandRegistry = CommandRegistry.create(new SimpleMeterRegistry());
    BulkheadCommandExecutor commandExecutor =
        new BulkheadCommandExecutor(new SimpleMeterRegistry());
    try {
      HealthManager healthManager = mock(HealthManager.class);
      when(healthManager.isHealthy()).thenReturn(false);
      ManageSearchIndexUnaryCallHandler handler =
          new ManageSearchIndexUnaryCallHandler(commandRegistry, commandExecutor, healthManager);
      StreamObserver<RawBsonDocument> responseObserver = mock(RawBsonDocumentStreamObserver.class);
      RawBsonDocument request = toRaw(new BsonDocument("manageSearchIndex", BsonBoolean.TRUE));

      handler.invoke(request, responseObserver);

      ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
      verify(responseObserver, timeout(5000)).onError(captor.capture());
      verify(responseObserver, never()).onNext(any());
      verify(responseObserver, never()).onCompleted();
      Assert.assertTrue(captor.getValue() instanceof StatusRuntimeException);
      Assert.assertEquals(
          Status.Code.UNAVAILABLE,
          ((StatusRuntimeException) captor.getValue()).getStatus().getCode());
    } finally {
      commandExecutor.close();
    }
  }
}
