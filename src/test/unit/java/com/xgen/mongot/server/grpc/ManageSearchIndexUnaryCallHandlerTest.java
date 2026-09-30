package com.xgen.mongot.server.grpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import com.xgen.mongot.util.ErrorType;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.testing.TestUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

public class ManageSearchIndexUnaryCallHandlerTest {

  private static final String TOTAL_LATENCY_METRIC = "command.manageSearchIndexCommandTotalLatency";
  private static final String FAILURE_METRIC = "command.manageSearchIndexCommandFailure";

  private interface RawBsonDocumentStreamObserver extends StreamObserver<RawBsonDocument> {}

  private static CommandFactory commandFactory(boolean throwsOnRun) {
    return args ->
        new Command() {
          @Override
          public String name() {
            return "manageSearchIndex";
          }

          @Override
          public BsonDocument run() {
            if (throwsOnRun) {
              throw new RuntimeException("boom");
            }
            return args;
          }

          @Override
          public ExecutionPolicy getExecutionPolicy() {
            return ExecutionPolicy.SYNC;
          }
        };
  }

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
      commandRegistry.registerCommand("manageSearchIndex", commandFactory(false), true);
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
      List<ILoggingEvent> logEvents =
          TestUtils.getLogEvents(
              (Logger) LoggerFactory.getLogger(ManageSearchIndexUnaryCallHandler.class));

      handler.invoke(request, responseObserver);

      // An unknown command is expected during version skew: answered with a code, never logged
      // as an error.
      Assert.assertTrue(logEvents.stream().noneMatch(event -> event.getLevel() == Level.ERROR));

      ArgumentCaptor<RawBsonDocument> captor = ArgumentCaptor.forClass(RawBsonDocument.class);
      verify(responseObserver, timeout(5000)).onNext(captor.capture());
      verify(responseObserver, timeout(5000)).onCompleted();
      verify(responseObserver, never()).onError(any());
      Assert.assertEquals(0, captor.getValue().getInt32("ok").getValue());
      Assert.assertEquals(
          "no command registered for unknownCommand",
          captor.getValue().getString("errmsg").getValue());
      Assert.assertEquals(
          Errors.COMMAND_NOT_FOUND.code, captor.getValue().getInt32("code").getValue());
      Assert.assertEquals(
          Errors.COMMAND_NOT_FOUND.name, captor.getValue().getString("codeName").getValue());
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
      commandRegistry.registerCommand("manageSearchIndex", commandFactory(true), true);
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

  @Test
  public void invoke_success_recordsTotalLatency() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    CommandRegistry commandRegistry = CommandRegistry.create(meterRegistry);
    BulkheadCommandExecutor commandExecutor =
        new BulkheadCommandExecutor(new SimpleMeterRegistry());
    try {
      // Registered without detailed stats, same as SearchCommandsRegister.
      commandRegistry.registerCommand("manageSearchIndex", commandFactory(false), false);
      ManageSearchIndexUnaryCallHandler handler = createHandler(commandRegistry, commandExecutor);
      StreamObserver<RawBsonDocument> responseObserver = mock(RawBsonDocumentStreamObserver.class);

      handler.invoke(
          toRaw(new BsonDocument("manageSearchIndex", BsonBoolean.TRUE)), responseObserver);

      verify(responseObserver, timeout(5000)).onCompleted();
      Timer totalTimer = meterRegistry.find(TOTAL_LATENCY_METRIC).timer();
      Assert.assertNotNull(totalTimer);
      Assert.assertEquals(1, totalTimer.count());
      Assert.assertNull(meterRegistry.find(FAILURE_METRIC).counter());
    } finally {
      commandExecutor.close();
    }
  }

  @Test
  public void invoke_commandThrows_recordsFailure() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    CommandRegistry commandRegistry = CommandRegistry.create(meterRegistry);
    BulkheadCommandExecutor commandExecutor =
        new BulkheadCommandExecutor(new SimpleMeterRegistry());
    try {
      commandRegistry.registerCommand("manageSearchIndex", commandFactory(true), false);
      ManageSearchIndexUnaryCallHandler handler = createHandler(commandRegistry, commandExecutor);
      StreamObserver<RawBsonDocument> responseObserver = mock(RawBsonDocumentStreamObserver.class);

      handler.invoke(
          toRaw(new BsonDocument("manageSearchIndex", BsonBoolean.TRUE)), responseObserver);

      verify(responseObserver, timeout(5000)).onCompleted();
      Counter failureCounter =
          meterRegistry
              .find(FAILURE_METRIC)
              .tag("exceptionName", "RuntimeException")
              .tag(ErrorType.METRIC_TAG_KEY, ErrorType.INTERNAL_ERROR.toString())
              .counter();
      Assert.assertNotNull(failureCounter);
      Assert.assertEquals(1.0, failureCounter.count(), 0.0);
      Assert.assertEquals(0, meterRegistry.find(TOTAL_LATENCY_METRIC).timer().count());
    } finally {
      commandExecutor.close();
    }
  }

  @Test
  public void invoke_commandCreationThrows_recordsFailure() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    CommandRegistry commandRegistry = CommandRegistry.create(meterRegistry);
    BulkheadCommandExecutor commandExecutor =
        new BulkheadCommandExecutor(new SimpleMeterRegistry());
    try {
      commandRegistry.registerCommand(
          "manageSearchIndex",
          (CommandFactory)
              args -> {
                throw new IllegalArgumentException("bad request");
              },
          false);
      ManageSearchIndexUnaryCallHandler handler = createHandler(commandRegistry, commandExecutor);
      StreamObserver<RawBsonDocument> responseObserver = mock(RawBsonDocumentStreamObserver.class);

      handler.invoke(
          toRaw(new BsonDocument("manageSearchIndex", BsonBoolean.TRUE)), responseObserver);

      verify(responseObserver, timeout(5000)).onCompleted();
      Counter failureCounter =
          meterRegistry
              .find(FAILURE_METRIC)
              .tag("exceptionName", "IllegalArgumentException")
              .counter();
      Assert.assertNotNull(failureCounter);
      Assert.assertEquals(1.0, failureCounter.count(), 0.0);
    } finally {
      commandExecutor.close();
    }
  }
}
