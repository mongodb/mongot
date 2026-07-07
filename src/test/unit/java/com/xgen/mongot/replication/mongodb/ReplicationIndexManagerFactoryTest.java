package com.xgen.mongot.replication.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.replication.mongodb.RecyclableReplicationIndexManager.LeaderStatus;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Test;

public class ReplicationIndexManagerFactoryTest {

  @Test
  public void forIndexGeneration_vectorLiteIndex_doesNotReturnDefaultFactory() {
    var vectorLiteDef =
        VectorIndexDefinitionBuilder.builder().withCustomEngineVectorField("vector", 3).build();
    var indexGeneration = mockIndexGeneration(vectorLiteDef);
    var defaultFactory = mock(ReplicationIndexManagerFactory.class);

    var factory =
        ReplicationIndexManagerFactory.forIndexGeneration(
            indexGeneration, defaultFactory, Optional.empty());

    assertThat(factory).isNotSameInstanceAs(defaultFactory);
  }

  @Test
  public void forIndexGeneration_nonCustomVectorIndex_returnsDefaultFactory() {
    var stdVectorDef =
        VectorIndexDefinitionBuilder.builder().withCosineVectorField("vector", 3).build();
    var indexGeneration = mockIndexGeneration(stdVectorDef);
    var defaultFactory = mock(ReplicationIndexManagerFactory.class);

    var factory =
        ReplicationIndexManagerFactory.forIndexGeneration(
            indexGeneration, defaultFactory, Optional.empty());

    assertThat(factory).isSameInstanceAs(defaultFactory);
  }

  @Test
  public void forIndexGeneration_nonVectorIndex_returnsDefaultFactory() {
    var indexGeneration = mockIndexGeneration(SearchIndexDefinitionBuilder.VALID_INDEX);
    var defaultFactory = mock(ReplicationIndexManagerFactory.class);

    var factory =
        ReplicationIndexManagerFactory.forIndexGeneration(
            indexGeneration, defaultFactory, Optional.empty());

    assertThat(factory).isSameInstanceAs(defaultFactory);
  }

  @Test
  public void forIndexGeneration_customVectorIndexWithRegistrar_registersManagerSinkOnCreate() {
    var vectorLiteDef =
        VectorIndexDefinitionBuilder.builder().withCustomEngineVectorField("vector", 3).build();
    var indexGeneration = mockIndexGeneration(vectorLiteDef);
    var defaultFactory = mock(ReplicationIndexManagerFactory.class);

    var registeredGeneration = new AtomicReference<IndexGeneration>();
    var registeredSink = new AtomicReference<Consumer<LeaderStatus>>();
    LeaderStatusRegistrar registrar =
        (generation, sink) -> {
          registeredGeneration.set(generation);
          registeredSink.set(sink);
        };

    var factory =
        ReplicationIndexManagerFactory.forIndexGeneration(
            indexGeneration, defaultFactory, Optional.of(registrar));

    IndexManager manager =
        factory.create(
            mock(Executor.class),
            mock(MongotCursorManager.class),
            mock(InitialSyncQueue.class),
            mock(SteadyStateManager.class),
            Optional.empty(),
            indexGeneration,
            mock(InitializedVectorIndex.class),
            mock(DocumentIndexer.class),
            mock(ScheduledExecutorService.class),
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            mock(MeterRegistry.class),
            FeatureFlags.getDefault(),
            false);

    // The wrapping factory must create the recyclable manager and wire its leadership sink through
    // the registrar, so a real SLS source can drive it with no further wiring.
    assertThat(manager).isInstanceOf(RecyclableReplicationIndexManager.class);
    assertThat(registeredGeneration.get()).isSameInstanceAs(indexGeneration);
    assertThat(registeredSink.get()).isNotNull();
  }

  @Test
  public void forIndexGeneration_customVectorIndexWithoutRegistrar_throwsOnCreate() {
    var vectorLiteDef =
        VectorIndexDefinitionBuilder.builder().withCustomEngineVectorField("vector", 3).build();
    var indexGeneration = mockIndexGeneration(vectorLiteDef);
    var defaultFactory = mock(ReplicationIndexManagerFactory.class);

    var factory =
        ReplicationIndexManagerFactory.forIndexGeneration(
            indexGeneration, defaultFactory, Optional.empty());

    // A custom vector engine index is leadership-driven, so creating its manager without a
    // registrar
    // must fail loudly rather than silently skip leadership wiring.
    assertThrows(
        IllegalStateException.class,
        () ->
            factory.create(
                mock(Executor.class),
                mock(MongotCursorManager.class),
                mock(InitialSyncQueue.class),
                mock(SteadyStateManager.class),
                Optional.empty(),
                indexGeneration,
                mock(InitializedVectorIndex.class),
                mock(DocumentIndexer.class),
                mock(ScheduledExecutorService.class),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                mock(MeterRegistry.class),
                FeatureFlags.getDefault(),
                false));
  }
}
