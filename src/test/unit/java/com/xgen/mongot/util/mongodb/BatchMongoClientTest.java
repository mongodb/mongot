package com.xgen.mongot.util.mongodb;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.client.ClientSession;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.operation.ReadOperation;
import org.junit.Test;

public class BatchMongoClientTest {

  @Test
  public void testDefaultConnectionStringValues() {
    var connectionString = new ConnectionString("mongodb://localhost:37017/?authSource=admin");
    var settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

    try (var mongoClient = new BatchMongoClient(settings)) {
      var readOperation = mock(ReadOperation.class);
      var session = mock(ClientSession.class);
      when(session.getOptions()).thenReturn(ClientSessionOptions.builder().build());

      mongoClient.openCursor(readOperation, session);

      // Assert the connection-string defaults: primary read preference on the ReadBinding, and
      // the default read concern on the OperationContext's session context (driver 5.x threads
      // readConcern there rather than on the ReadBinding).
      verify(readOperation)
          .execute(
              argThat(binding -> checkReadPreference(binding, ReadPreference.primary())),
              argThat(operationContext -> checkReadConcern(operationContext, ReadConcern.DEFAULT)));
    }
  }

  @Test
  public void testReadPreferenceSettingProvidedByConnectionString() {
    var connectionString =
        new ConnectionString(
            "mongodb://localhost:37017/?authSource=admin"
                + "&readConcernLevel=majority"
                + "&readPreference=secondary");

    var settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

    try (var mongoClient = new BatchMongoClient(settings)) {
      var readOperation = mock(ReadOperation.class);
      var session = mock(ClientSession.class);
      when(session.getOptions()).thenReturn(ClientSessionOptions.builder().build());

      mongoClient.openCursor(readOperation, session);

      // Driver 5.x threads readConcern through the OperationContext's session context rather
      // than the ReadBinding, so assert each on its respective argument.
      verify(readOperation)
          .execute(
              argThat(binding -> checkReadPreference(binding, ReadPreference.secondary())),
              argThat(
                  operationContext -> checkReadConcern(operationContext, ReadConcern.MAJORITY)));
    }
  }

  private boolean checkReadPreference(ReadBinding binding, ReadPreference readPreference) {
    return binding.getReadPreference().equals(readPreference);
  }

  private boolean checkReadConcern(OperationContext operationContext, ReadConcern readConcern) {
    return operationContext.getSessionContext().getReadConcern().equals(readConcern);
  }

  @Test
  public void testMaxTimeMsProvided_appliedToTimeoutContext() {
    var connectionString = new ConnectionString("mongodb://localhost:37017/?authSource=admin");
    var settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

    try (var mongoClient = new BatchMongoClient(settings)) {
      var readOperation = mock(ReadOperation.class);
      var session = mock(ClientSession.class);
      when(session.getOptions()).thenReturn(ClientSessionOptions.builder().build());

      mongoClient.openCursor(readOperation, session, java.util.Optional.of(1234));

      // The per-operation maxTimeMs must be carried on the OperationContext's TimeoutSettings so
      // CommandHelper.applyMaxTimeMS emits it onto the command (driver 5.x applies maxTimeMS
      // centrally from the TimeoutContext rather than via a per-operation setter).
      verify(readOperation)
          .execute(
              argThat(binding -> checkReadPreference(binding, ReadPreference.primary())),
              argThat(
                  operationContext ->
                      operationContext.getTimeoutContext().getTimeoutSettings().getMaxTimeMS()
                          == 1234));
    }
  }

  @Test
  public void testMaxTimeMsAbsent_usesDefaultTimeoutSettings() {
    var connectionString = new ConnectionString("mongodb://localhost:37017/?authSource=admin");
    var settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

    try (var mongoClient = new BatchMongoClient(settings)) {
      var readOperation = mock(ReadOperation.class);
      var session = mock(ClientSession.class);
      when(session.getOptions()).thenReturn(ClientSessionOptions.builder().build());

      mongoClient.openCursor(readOperation, session, java.util.Optional.empty());

      // No per-operation override: the default TimeoutSettings carries maxTimeMS = 0 (driver
      // default meaning "no server-side limit"), which CommandHelper treats as unset.
      verify(readOperation)
          .execute(
              argThat(binding -> checkReadPreference(binding, ReadPreference.primary())),
              argThat(
                  operationContext ->
                      operationContext.getTimeoutContext().getTimeoutSettings().getMaxTimeMS()
                          == 0));
    }
  }

  @Test
  public void testContextProviderSet_propagatesToOperationContext() {
    // A configured SynchronousContextProvider must be honored (its RequestContext used), matching
    // the 4.x behavior — not silently dropped in favor of IgnorableRequestContext.
    RequestContext providedContext = mock(RequestContext.class);
    com.mongodb.client.SynchronousContextProvider provider = () -> providedContext;
    var settings =
        MongoClientSettings.builder()
            .applyConnectionString(
                new ConnectionString("mongodb://localhost:37017/?authSource=admin"))
            .contextProvider(provider)
            .build();

    try (var mongoClient = new BatchMongoClient(settings)) {
      var readOperation = mock(ReadOperation.class);
      var session = mock(ClientSession.class);
      when(session.getOptions()).thenReturn(ClientSessionOptions.builder().build());

      mongoClient.openCursor(readOperation, session);

      verify(readOperation)
          .execute(
              argThat(binding -> checkReadPreference(binding, ReadPreference.primary())),
              argThat(operationContext -> operationContext.getRequestContext() == providedContext));
    }
  }

  @Test
  public void testNoContextProvider_usesIgnorableRequestContext() {
    var settings =
        MongoClientSettings.builder()
            .applyConnectionString(
                new ConnectionString("mongodb://localhost:37017/?authSource=admin"))
            .build();

    try (var mongoClient = new BatchMongoClient(settings)) {
      var readOperation = mock(ReadOperation.class);
      var session = mock(ClientSession.class);
      when(session.getOptions()).thenReturn(ClientSessionOptions.builder().build());

      mongoClient.openCursor(readOperation, session);

      verify(readOperation)
          .execute(
              argThat(binding -> checkReadPreference(binding, ReadPreference.primary())),
              argThat(
                  operationContext ->
                      operationContext.getRequestContext()
                          == com.mongodb.internal.IgnorableRequestContext.INSTANCE));
    }
  }
}
