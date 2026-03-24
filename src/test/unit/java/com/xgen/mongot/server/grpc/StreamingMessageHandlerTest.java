package com.xgen.mongot.server.grpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.CommandFactoryMarker;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.ExecutorManager;
import com.xgen.mongot.server.executors.RegularBlockingRequestSettings;
import com.xgen.mongot.server.message.MessageHeader;
import com.xgen.mongot.server.message.MessageMessage;
import com.xgen.mongot.server.message.MessageSection;
import com.xgen.mongot.server.message.MessageSectionBody;
import com.xgen.mongot.server.message.OpCode;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class StreamingMessageHandlerTest {

  private interface MessageMessageStreamObserver extends StreamObserver<MessageMessage> {}

  final MeterRegistry meterRegistry = new SimpleMeterRegistry();
  final CommandRegistry commandRegistry = CommandRegistry.create(this.meterRegistry);
  final ExecutorManager executorManager = new ExecutorManager(this.meterRegistry);
  final StreamObserver<MessageMessage> responseObserver = mock(MessageMessageStreamObserver.class);

  final MongotCursorManager cursorManager = mock(MongotCursorManager.class);
  volatile boolean streamTerminated = false;

  final WireMessageCallHandler messageHandler =
      new WireMessageCallHandler(
          this.commandRegistry,
          this.executorManager.commandExecutor,
          this.cursorManager,
          SearchEnvoyMetadata.getDefaultInstance(),
          this.responseObserver);

  final InOrder inOrder = Mockito.inOrder(this.responseObserver, this.cursorManager);

  private static MessageMessage createOpMsgFromBsonDocument(BsonDocument document) {
    ArrayList<MessageSection> sections = new ArrayList<>();
    sections.add(new MessageSectionBody(document));
    return new MessageMessage(new MessageHeader(0, 233, 0, OpCode.MSG), 0, sections);
  }

  /**
   * Verifies that a single {@link MessageMessage} that wraps {@code expectedDocument} is sent to
   * the {@code responseObserver}.
   */
  private void verifyServerResponse(BsonDocument expectedDocument) {
    ArgumentCaptor<MessageMessage> captor = ArgumentCaptor.forClass(MessageMessage.class);
    this.inOrder.verify(this.responseObserver, timeout(5000).times(1)).onNext(captor.capture());
    Assert.assertEquals(
        expectedDocument, ((MessageSectionBody) captor.getValue().sections().get(0)).body);
  }

  /** Verifies that half-close is sent to the {@code responseObserver}. */
  private void verifyServerHalfClosed() {
    this.inOrder.verify(this.responseObserver, timeout(5000).times(1)).onCompleted();
  }

  /**
   * Verifies that a single {@link MessageMessage} that wraps an error message is sent to the {@code
   * responseObserver}.
   */
  private void verifyServerErrorResponse(String expectedErrorMessage) {
    ArgumentCaptor<MessageMessage> captor = ArgumentCaptor.forClass(MessageMessage.class);
    this.inOrder.verify(this.responseObserver, timeout(5000).times(1)).onNext(captor.capture());
    var bsonDocument = ((MessageSectionBody) captor.getValue().sections().get(0)).body;
    Assert.assertEquals(0, bsonDocument.getInt32("ok").getValue());
    Assert.assertEquals(expectedErrorMessage, bsonDocument.getString("errmsg").getValue());
  }

  /** Verifies that {@code responseObserver} is not called. */
  private void verifyNoResponseObserverCalls(long waitMillis) {
    verify(this.responseObserver, after(waitMillis).never()).onNext(any());
    verify(this.responseObserver, never()).onCompleted();
    verify(this.responseObserver, never()).onError(any());
  }

  private void verifyTimers(String commandName, int expectedCount) {
    var commandTimers =
        this.meterRegistry.getMeters().stream()
            .filter(meter -> meter instanceof CumulativeTimer)
            .filter(meter -> meter.getId().getName().startsWith("command."))
            .collect(
                Collectors.toMap(
                    meter -> meter.getId().getName().substring("command.".length()),
                    meter -> (CumulativeTimer) meter));
    CumulativeTimer totalLatencyTimer = commandTimers.get(commandName + "CommandTotalLatency");
    Assert.assertNotNull(totalLatencyTimer);
    Assert.assertEquals(expectedCount, totalLatencyTimer.count());
    CumulativeTimer serializationLatencyTimer =
        commandTimers.get(commandName + "CommandSerializationLatency");
    Assert.assertNotNull(serializationLatencyTimer);
    Assert.assertEquals(expectedCount, serializationLatencyTimer.count());
  }

  private void verifyKilledCursors(long... cursorIds) {
    Arrays.stream(cursorIds)
        .forEach(
            (cursorId) -> {
              this.inOrder.verify(this.cursorManager, timeout(5000).times(1)).killCursor(cursorId);
            });
  }

  @Before
  public void mockStreamTermination() {
    Answer<Void> answerVoidMethod =
        invocation -> {
          if (this.streamTerminated) {
            throw Status.CANCELLED.asRuntimeException();
          }
          return null;
        };
    doAnswer(answerVoidMethod).when(this.responseObserver).onNext(any());
    doAnswer(answerVoidMethod).when(this.responseObserver).onCompleted();
  }

  @After
  public void noMoreInteractions() {
    verifyNoMoreInteractions(this.responseObserver);
    verifyNoMoreInteractions(this.cursorManager);
  }

  private static class IdentityCommand implements Command {

    private static final String Name = "identity";
    private final BsonDocument args;
    private final ExecutionPolicy executionPolicy;

    private IdentityCommand(BsonDocument args, ExecutionPolicy executionPolicy) {
      this.args = args;
      this.executionPolicy = executionPolicy;
    }

    @Override
    public String name() {
      return Name;
    }

    @Override
    public BsonDocument run() {
      return this.args;
    }

    @Override
    public ExecutionPolicy getExecutionPolicy() {
      return this.executionPolicy;
    }
  }

  private static class LongRunningCommand implements Command {

    private static final String Name = "longRunning";
    private final CountDownLatch latch;
    private final CountDownLatch startedLatch;

    private LongRunningCommand(CountDownLatch latch) {
      this(latch, null);
    }

    private LongRunningCommand(CountDownLatch latch, CountDownLatch startedLatch) {
      this.latch = latch;
      this.startedLatch = startedLatch;
    }

    @Override
    public String name() {
      return Name;
    }

    @Override
    public BsonDocument run() {
      if (this.startedLatch != null) {
        this.startedLatch.countDown();
      }
      try {
        this.latch.await();
      } catch (InterruptedException e) {
        // Do nothing.
      }
      return new BsonDocument();
    }

    @Override
    public ExecutionPolicy getExecutionPolicy() {
      return ExecutionPolicy.ASYNC;
    }
  }

  private static class ThrowExceptionCommand implements Command {

    private static final String Name = "throwExceptionCommand";
    private final ExecutionPolicy executionPolicy;

    private ThrowExceptionCommand(ExecutionPolicy executionPolicy) {
      this.executionPolicy = executionPolicy;
    }

    @Override
    public String name() {
      return Name;
    }

    @Override
    public BsonDocument run() {
      throw new RuntimeException("throw exception by command");
    }

    @Override
    public ExecutionPolicy getExecutionPolicy() {
      return this.executionPolicy;
    }
  }

  private static class CreateCursorsCommand implements Command {

    private static final String Name = "createCursorsCommand";

    private static final long SEARCH_CURSOR_ID = 123;
    private static final long META_CURSOR_ID = 456;

    private final List<Long> createdCursorIds;

    private CreateCursorsCommand() {
      this.createdCursorIds = new ArrayList<Long>();
    }

    @Override
    public String name() {
      return Name;
    }

    @Override
    public BsonDocument run() {
      this.createdCursorIds.add(SEARCH_CURSOR_ID);
      this.createdCursorIds.add(META_CURSOR_ID);
      return new BsonDocument();
    }

    @Override
    public List<Long> getCreatedCursorIds() {
      return this.createdCursorIds;
    }

    @Override
    public ExecutionPolicy getExecutionPolicy() {
      return ExecutionPolicy.ASYNC;
    }
  }

  private static class DependOnCursorsCommand implements Command {

    private static final String Name = "dependOnCursorsCommand";

    private DependOnCursorsCommand() {}

    @Override
    public String name() {
      return Name;
    }

    @Override
    public BsonDocument run() {
      return new BsonDocument();
    }

    @Override
    public boolean dependOnCursors() {
      return true;
    }

    @Override
    public ExecutionPolicy getExecutionPolicy() {
      return ExecutionPolicy.ASYNC;
    }
  }

  @Test
  public void handleSyncCommand() {
    this.commandRegistry.registerCommand(
        IdentityCommand.Name,
        (CommandFactory) args -> new IdentityCommand(args, Command.ExecutionPolicy.SYNC),
        true);
    BsonDocument document = new BsonDocument().append(IdentityCommand.Name, new BsonString("foo"));
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    verifyServerResponse(document);
    verifyTimers(IdentityCommand.Name, 1);

    this.messageHandler.onCompleted();
    verifyServerHalfClosed();
  }

  @Test
  public void handleAsyncCommand() {
    this.commandRegistry.registerCommand(
        IdentityCommand.Name,
        (CommandFactory) args -> new IdentityCommand(args, Command.ExecutionPolicy.ASYNC),
        true);
    BsonDocument document = new BsonDocument().append(IdentityCommand.Name, new BsonString("bar"));
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    verifyServerResponse(document);

    this.messageHandler.onCompleted();
    verifyServerHalfClosed();

    // Shutdown the command executor, so that metrics are populated. Otherwise, the metrics might be
    // flaky. Only async commands need this.
    this.executorManager.commandExecutor.close();
    verifyTimers(IdentityCommand.Name, 1);
  }

  @Test
  public void invalidCommand() {
    this.messageHandler.onNext(createOpMsgFromBsonDocument(new BsonDocument()));
    verifyServerErrorResponse("invalid command format; expected at least one body key");

    this.messageHandler.onCompleted();
    verifyServerHalfClosed();
  }

  @Test
  public void invalidCommandName() {
    BsonDocument document = new BsonDocument().append("invalidCommandName", BsonBoolean.TRUE);
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    verifyServerErrorResponse("no command registered for invalidCommandName");

    this.messageHandler.onCompleted();
    verifyServerHalfClosed();
  }

  @Test
  public void invalidFactoryType() {
    String testSessionCommandName = "saslStart";
    this.commandRegistry.registerInsecureCommand(
        testSessionCommandName,
        () -> CommandFactoryMarker.Type.SESSION_COMMAND_FACTORY,
        true);
    BsonDocument document =
        new BsonDocument().append(testSessionCommandName, new BsonString("foo"));
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    verifyServerErrorResponse(
        "do not know how to work with the command factory of " + testSessionCommandName);
    Assert.assertEquals(
        1.00,
        this.commandRegistry.getCommandRegistration(testSessionCommandName).failureCounter.count(),
        0.0);

    this.messageHandler.onCompleted();
    verifyServerHalfClosed();
  }

  @Test
  public void throwExceptionInSyncCommand() {
    this.commandRegistry.registerCommand(
        ThrowExceptionCommand.Name,
        (CommandFactory) ignored -> new ThrowExceptionCommand(Command.ExecutionPolicy.SYNC),
        true);
    BsonDocument document = new BsonDocument().append(ThrowExceptionCommand.Name, BsonBoolean.TRUE);
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    verifyServerErrorResponse("throw exception by command");
    Assert.assertEquals(
        1.00,
        this.commandRegistry
            .getCommandRegistration(ThrowExceptionCommand.Name)
            .failureCounter
            .count(),
        0.0);

    this.messageHandler.onCompleted();
    verifyServerHalfClosed();
  }

  @Test
  public void throwExceptionInAsyncCommand() {
    this.commandRegistry.registerCommand(
        ThrowExceptionCommand.Name,
        (CommandFactory) ignored -> new ThrowExceptionCommand(Command.ExecutionPolicy.ASYNC),
        true);
    BsonDocument document = new BsonDocument().append(ThrowExceptionCommand.Name, BsonBoolean.TRUE);
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    verifyServerErrorResponse("throw exception by command");
    this.messageHandler.onCompleted();
    verifyServerHalfClosed();

    // shut down the command executor for metrics to get updated for async commands to avoid
    // intermittent failure of the assertion
    this.executorManager.commandExecutor.close();
    Assert.assertEquals(
        1.00,
        this.commandRegistry
            .getCommandRegistration(ThrowExceptionCommand.Name)
            .failureCounter
            .count(),
        0.0);
  }

  @Test
  public void clientSendHalfCloseBeforeSendingCommands() {
    this.messageHandler.onCompleted();
    verifyServerHalfClosed();
  }

  @Test
  public void clientSendHalfCloseWhenThereIsARunningCommand() {
    CountDownLatch latch = new CountDownLatch(1);
    this.commandRegistry.registerCommand(
        LongRunningCommand.Name, (CommandFactory) ignored -> new LongRunningCommand(latch), true);
    BsonDocument document = new BsonDocument().append(LongRunningCommand.Name, BsonBoolean.TRUE);
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    this.messageHandler.onCompleted();

    // Since the command is still running, server should not send response or half-close.
    verifyNoResponseObserverCalls(5000);

    // This call will let the longRunning finish.
    latch.countDown();

    verifyServerResponse(new BsonDocument());
    verifyServerHalfClosed();
  }

  @Test
  public void clientSendHalfCloseAfterSendingMultipleCommands() {
    this.commandRegistry.registerCommand(
        IdentityCommand.Name,
        (CommandFactory) args -> new IdentityCommand(args, Command.ExecutionPolicy.ASYNC),
        true);
    BsonDocument document = new BsonDocument().append(IdentityCommand.Name, BsonBoolean.TRUE);
    for (@Var int i = 0; i < 10; ++i) {
      this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    }
    this.messageHandler.onCompleted();

    this.inOrder.verify(this.responseObserver, timeout(5000).times(10)).onNext(any());
    verifyServerHalfClosed();
  }

  @Test
  public void streamTermination() {
    this.commandRegistry.registerCommand(
        CreateCursorsCommand.Name, (CommandFactory) args -> new CreateCursorsCommand(), true);
    BsonDocument document = new BsonDocument().append(CreateCursorsCommand.Name, BsonBoolean.TRUE);
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    verifyServerResponse(new BsonDocument());
    doThrow(Status.CANCELLED.asRuntimeException()).when(this.responseObserver).onCompleted();
    this.messageHandler.onError(Status.CANCELLED.asRuntimeException());
    verifyServerHalfClosed();
    verifyKilledCursors(CreateCursorsCommand.SEARCH_CURSOR_ID, CreateCursorsCommand.META_CURSOR_ID);
  }

  @Test
  public void streamTerminationBeforeSendingCommands() {
    this.streamTerminated = true;
    this.messageHandler.onError(Status.CANCELLED.asRuntimeException());
    verifyServerHalfClosed();
  }

  @Test
  public void streamTerminationWhenThereIsARunningCommand() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch startedLatch = new CountDownLatch(1);
    this.commandRegistry.registerCommand(
        LongRunningCommand.Name,
        (CommandFactory) ignored -> new LongRunningCommand(latch, startedLatch),
        true);
    BsonDocument document = new BsonDocument().append(LongRunningCommand.Name, BsonBoolean.TRUE);
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    Assert.assertTrue(startedLatch.await(5, TimeUnit.SECONDS));
    this.streamTerminated = true;
    this.messageHandler.onError(Status.CANCELLED.asRuntimeException());

    // Since the command is still running, server should not send response or half-close.
    verifyNoResponseObserverCalls(5000);

    // This call will let the longRunning finish.
    latch.countDown();

    verifyServerResponse(new BsonDocument());
    verifyServerHalfClosed();
  }

  @Test
  public void streamTerminationAfterSendingMultipleCommands() {
    this.commandRegistry.registerCommand(
        IdentityCommand.Name,
        (CommandFactory) args -> new IdentityCommand(args, Command.ExecutionPolicy.ASYNC),
        true);
    BsonDocument document = new BsonDocument().append(IdentityCommand.Name, BsonBoolean.TRUE);
    for (@Var int i = 0; i < 10; ++i) {
      this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    }
    this.streamTerminated = true;
    this.messageHandler.onError(Status.CANCELLED.asRuntimeException());

    this.inOrder.verify(this.responseObserver, timeout(5000).times(10)).onNext(any());
    verifyServerHalfClosed();
  }

  @Test
  public void throwExceptionInDependOnCursorsCommand() {
    this.commandRegistry.registerCommand(
        DependOnCursorsCommand.Name,
        (CommandFactory) ignored -> new DependOnCursorsCommand(),
        true);
    BsonDocument document =
        new BsonDocument().append(DependOnCursorsCommand.Name, BsonBoolean.TRUE);
    this.messageHandler.onNext(createOpMsgFromBsonDocument(document));
    verifyServerErrorResponse("gRPC stream is broken");
    this.messageHandler.onCompleted();
    verifyServerHalfClosed();
  }

  @Test
  public void handleDependOnCursorsCommand() {
    this.commandRegistry.registerCommand(
        CreateCursorsCommand.Name, (CommandFactory) ignored -> new CreateCursorsCommand(), true);
    this.commandRegistry.registerCommand(
        DependOnCursorsCommand.Name,
        (CommandFactory) ignored -> new DependOnCursorsCommand(),
        true);
    this.messageHandler.onNext(
        createOpMsgFromBsonDocument(
            new BsonDocument().append(CreateCursorsCommand.Name, BsonBoolean.TRUE)));
    verifyServerResponse(new BsonDocument());
    this.messageHandler.onNext(
        createOpMsgFromBsonDocument(
            new BsonDocument().append(DependOnCursorsCommand.Name, BsonBoolean.TRUE)));
    verifyServerResponse(new BsonDocument());
    this.messageHandler.onCompleted();
    verifyServerHalfClosed();
  }

  @Test
  public void onNext_executorRejectsCommand_returnsRejectionErrorToClient()
      throws InterruptedException {
    // Create a bounded executor with minimal pool/queue size to trigger rejection.
    // The 0.001 multiplier ensures poolSize=1 and queueCapacity=1 regardless of CPU count
    // (poolSize = ceil(0.001 * numCpus) = 1), making it easy to fill and trigger rejection.
    SimpleMeterRegistry boundedMeterRegistry = new SimpleMeterRegistry();
    CommandRegistry boundedCommandRegistry = CommandRegistry.create(boundedMeterRegistry);
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(0.001), Optional.of(0.001), Optional.of(false));
    ExecutorManager boundedExecutorManager =
        new ExecutorManager(boundedMeterRegistry, settings);

    StreamObserver<MessageMessage> boundedResponseObserver =
        mock(MessageMessageStreamObserver.class);
    WireMessageCallHandler boundedHandler =
        new WireMessageCallHandler(
            boundedCommandRegistry,
            boundedExecutorManager.commandExecutor,
            this.cursorManager,
            SearchEnvoyMetadata.getDefaultInstance(),
            boundedResponseObserver);

    CountDownLatch blockingLatch = new CountDownLatch(1);
    CountDownLatch taskStartedLatch = new CountDownLatch(1);

    // Register a blocking command that signals when it starts
    boundedCommandRegistry.registerCommand(
        "blockingCommand",
        (CommandFactory)
            ignored ->
                new Command() {
                  @Override
                  public String name() {
                    return "blockingCommand";
                  }

                  @Override
                  public BsonDocument run() {
                    taskStartedLatch.countDown();
                    try {
                      blockingLatch.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                    return new BsonDocument();
                  }

                  @Override
                  public ExecutionPolicy getExecutionPolicy() {
                    return ExecutionPolicy.ASYNC;
                  }
                },
        true);

    // Register a simple async command that will be rejected
    boundedCommandRegistry.registerCommand(
        IdentityCommand.Name,
        (CommandFactory) args -> new IdentityCommand(args, Command.ExecutionPolicy.ASYNC),
        true);

    try {
      // Send first command to occupy the single worker thread
      BsonDocument blockingDoc = new BsonDocument().append("blockingCommand", BsonBoolean.TRUE);
      boundedHandler.onNext(createOpMsgFromBsonDocument(blockingDoc));
      taskStartedLatch.await(5, TimeUnit.SECONDS);

      // Send second command to fill the queue
      BsonDocument queueFillerDoc =
          new BsonDocument().append(IdentityCommand.Name, BsonBoolean.TRUE);
      boundedHandler.onNext(createOpMsgFromBsonDocument(queueFillerDoc));

      // Small delay to ensure queue is populated
      Thread.sleep(50);

      // Send third command that should be rejected
      BsonDocument rejectedDoc =
          new BsonDocument().append(IdentityCommand.Name, new BsonString("should_be_rejected"));
      boundedHandler.onNext(createOpMsgFromBsonDocument(rejectedDoc));

      // Verify the rejection error is returned to the client
      ArgumentCaptor<MessageMessage> captor = ArgumentCaptor.forClass(MessageMessage.class);
      verify(boundedResponseObserver, timeout(5000).atLeast(1)).onNext(captor.capture());

      // Find the rejection error response and verify error labels
      @Var BsonDocument rejectionErrorDoc = null;
      for (MessageMessage msg : captor.getAllValues()) {
        BsonDocument bsonDocument = ((MessageSectionBody) msg.sections().get(0)).body;
        if (bsonDocument.containsKey("ok")
            && bsonDocument.getInt32("ok").getValue() == 0
            && bsonDocument.containsKey("errmsg")) {
          String errorMsg = bsonDocument.getString("errmsg").getValue();
          if (errorMsg.contains("Query rejected")
              && errorMsg.contains("currently at capacity")) {
            rejectionErrorDoc = bsonDocument;
            break;
          }
        }
      }
      Assert.assertNotNull(
          "Expected rejection error message to be returned to client", rejectionErrorDoc);

      // Verify error code and codeName are present for load shedding rejections
      Assert.assertTrue(
          "Expected code field in rejection response", rejectionErrorDoc.containsKey("code"));
      Assert.assertEquals(
          "Expected error code 462 (IngressRequestRateLimitExceeded)",
          462,
          rejectionErrorDoc.getInt32("code").getValue());
      Assert.assertTrue(
          "Expected codeName field in rejection response",
          rejectionErrorDoc.containsKey("codeName"));
      Assert.assertEquals(
          "Expected codeName IngressRequestRateLimitExceeded",
          "IngressRequestRateLimitExceeded",
          rejectionErrorDoc.getString("codeName").getValue());

      // Verify error labels are present for load shedding rejections
      Assert.assertTrue(
          "Expected errorLabels field in rejection response",
          rejectionErrorDoc.containsKey("errorLabels"));
      var errorLabels = rejectionErrorDoc.getArray("errorLabels");
      Assert.assertEquals(
          "Expected exactly 2 error labels (SystemOverloadedError and RetryableError)",
          2,
          errorLabels.size());

      // Verify both labels are present
      @Var boolean hasSystemOverloadedError = false;
      @Var boolean hasRetryableError = false;
      for (int i = 0; i < errorLabels.size(); i++) {
        String label = errorLabels.get(i).asString().getValue();
        if ("SystemOverloadedError".equals(label)) {
          hasSystemOverloadedError = true;
        }
        if ("RetryableError".equals(label)) {
          hasRetryableError = true;
        }
      }
      Assert.assertTrue(
          "Expected SystemOverloadedError label in rejection response", hasSystemOverloadedError);
      Assert.assertTrue(
          "Expected RetryableError label in rejection response", hasRetryableError);
    } finally {
      blockingLatch.countDown();
      boundedExecutorManager.commandExecutor.close();
    }
  }
}
