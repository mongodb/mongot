package com.xgen.testing.mongot.config.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.google.errorprone.annotations.Keep;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.analyzer.AnalyzerRegistryFactory;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.lifecycle.LifecycleManager;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.replication.ReplicationManagerFactory;
import com.xgen.mongot.util.FileUtils;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.testing.TestUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

/**
 * Couples a DefaultConfigManager with mocked/spied versions of its dependencies so that they can be
 * inspected for proper behavior.
 */
public class DefaultConfigManagerMocks {
  public final IndexCatalog indexCatalog;
  public final MongotCursorManager cursorManager;
  public final LifecycleManager lifecycleManager;
  @Keep final AnalyzerRegistryFactory analyzerRegistryFactory;
  public final IndexFactory indexFactory;
  public final Path configJournalPath;
  @Keep public final MeterRegistry meterRegistry;
  public final ReplicationManagerFactory replicationManagerFactory;
  public final Path dynamicFeatureFlagsJournalPath;

  public final DefaultConfigManager configManager;
  public final ConfigStateMocks mockedDependencies;

  private DefaultConfigManagerMocks(
      ConfigStateMocks mockedDependencies,
      ReplicationManagerFactory replicationManagerFactory,
      Optional<Set<ObjectId>> desiredIndexIds,
      ReplicationStateMonitor replicationStateMonitor) {
    this(mockedDependencies, replicationManagerFactory, desiredIndexIds, replicationStateMonitor,
        FeatureFlags.getDefault());
  }

  private DefaultConfigManagerMocks(
      ConfigStateMocks mockedDependencies,
      ReplicationManagerFactory replicationManagerFactory,
      Optional<Set<ObjectId>> desiredIndexIds,
      ReplicationStateMonitor replicationStateMonitor,
      FeatureFlags featureFlags) {
    this.mockedDependencies = mockedDependencies;
    this.indexCatalog = mockedDependencies.indexCatalog;
    this.cursorManager = mockedDependencies.cursorManager;
    this.lifecycleManager = mockedDependencies.lifecycleManager;
    this.analyzerRegistryFactory = mockedDependencies.analyzerRegistryFactory;
    this.indexFactory = mockedDependencies.indexFactory;
    this.configJournalPath = mockedDependencies.configJournalPath;
    this.meterRegistry = new SimpleMeterRegistry();
    this.replicationManagerFactory = replicationManagerFactory;
    this.dynamicFeatureFlagsJournalPath =
        mockedDependencies.configJournalPath.resolveSibling("dynamicFeatureFlagsJournal.json");

    this.configManager =
        DefaultConfigManager.initialize(
            mockedDependencies.configState,
            desiredIndexIds,
            this.configJournalPath,
            replicationStateMonitor,
            featureFlags,
            this.meterRegistry,
            this.dynamicFeatureFlagsJournalPath);
  }

  public static Path getConfigJournalPath() throws Exception {
    TemporaryFolder folder = TestUtils.getTempFolder();
    return Paths.get(folder.getRoot().toPath().toString(), "configJournal.json");
  }

  /** Resets the mocks verification counters so they can be re-verified. */
  public void clearInvocations() {
    this.mockedDependencies.clearInvocations();
  }

  public static DefaultConfigManagerMocks create() throws Exception {
    return create(getConfigJournalPath());
  }

  public static DefaultConfigManagerMocks create(FeatureFlags featureFlags) throws Exception {
    ConfigStateMocks wrapped = ConfigStateMocks.create(getConfigJournalPath(), false);
    wrapped.configState.postInitializationFromConfigJournal();

    return new DefaultConfigManagerMocks(
        wrapped, wrapped.getReplicationManagerFactory(), Optional.empty(),
        ReplicationStateMonitor.enabled(), featureFlags);
  }

  public static DefaultConfigManagerMocks create(
      ReplicationStateMonitor replicationStateMonitor)
      throws Exception {
    return create(getConfigJournalPath(), Optional.empty(), replicationStateMonitor);
  }

  public static DefaultConfigManagerMocks create(Path configJournalPath) throws Exception {
    return create(configJournalPath, Optional.empty());
  }

  /** Creates initialized mocks with a present config of any version. */
  public static DefaultConfigManagerMocks create(Encodable config) throws Exception {
    Path path = getConfigJournalPath();
    FileUtils.atomicallyReplace(path, JsonCodec.toJson(config.toBson().asDocument()));
    return create(path, Optional.empty());
  }

  public static DefaultConfigManagerMocks create(Encodable config, Set<ObjectId> desiredIndexIds)
      throws Exception {
    Path path = getConfigJournalPath();
    FileUtils.atomicallyReplace(path, JsonCodec.toJson(config.toBson().asDocument()));
    return create(path, Optional.of(desiredIndexIds));
  }

  static DefaultConfigManagerMocks create(
      Path configJournalPath, Optional<Set<ObjectId>> desiredIndexIds) throws Exception {
    var replicationStateMonitor = ReplicationStateMonitor.enabled();
    return create(configJournalPath, desiredIndexIds, replicationStateMonitor);
  }

  static DefaultConfigManagerMocks create(
      Path configJournalPath,
      Optional<Set<ObjectId>> desiredIndexIds,
      ReplicationStateMonitor replicationStateMonitor)
      throws Exception {
    ConfigStateMocks wrapped = ConfigStateMocks.create(configJournalPath, false);
    wrapped.configState.postInitializationFromConfigJournal();

    return new DefaultConfigManagerMocks(
        wrapped, wrapped.getReplicationManagerFactory(), desiredIndexIds, replicationStateMonitor);
  }

  public void assertOneIndexInstantiated() throws Exception {
    this.mockedDependencies.assertOneIndexCreated();
  }

  public void assertIndexCatalogedAndReplicated() throws Exception {
    this.mockedDependencies.assertOneIndexCreated();
    this.mockedDependencies.assertOneIndexCatalogedAndReplicated();
  }

  public void assertNoIndexActivity() {
    this.mockedDependencies.assertNoIndexActivity();
  }

  /**
   * Asserts that no indexes were created, and no behavior associated with indexes being created
   * occurred.
   */
  public void assertNoIndexesAdded() {
    this.mockedDependencies.assertNoIndexCatalogedAndReplicated();
  }

  /**
   * Helper that asserts the proper behavior of the DefaultConfigManager when there is either no
   * config journal or it is empty.
   */
  public void assertEmptyOrNoConfigJournalInit() throws Exception {
    // Shouldn't have tried to create any Indexes or effect the index catalog.
    assertNoIndexActivity();
    assertEquals("index catalog has indexes", 0, this.indexCatalog.getIndexes().size());

    // Fine to either have an empty or non existent config journal
    if (this.configJournalPath.toFile().exists()) {
      assertEmptyConfigJournal();
    }

    assertConfigUpdated(0, 0);
  }

  public void assertEmptyConfigJournal() throws Exception {
    this.mockedDependencies.assertPersistedJournalEmpty();
  }

  public void assertConfigUpdated(int expectedIndexInfos, int expectedGroupedIndexInfos) {
    assertEquals(
        String.format("should have %d index infos", expectedIndexInfos),
        expectedIndexInfos,
        this.configManager.getIndexInfos().size());
    assertEquals(
        String.format("should have %d grouped index infos", expectedGroupedIndexInfos),
        expectedGroupedIndexInfos,
        this.configManager.getGroupedIndexGenerationMetrics().size());
  }

  public void assertConfigUpdated(
      int expectedIndexInfos, int expectedGroupedIndexInfos, ConfigJournalV1 expectedConfigJournal)
      throws Exception {
    assertConfigUpdated(expectedIndexInfos, expectedGroupedIndexInfos);

    Assert.assertEquals(expectedConfigJournal, this.getConfigJournalV1());
  }

  ConfigJournalV1 getConfigJournalV1() throws Exception {
    Optional<ConfigJournalV1> optionalConfigJournal =
        ConfigJournalV1.fromFileIfExists(this.configJournalPath);
    Assert.assertTrue("cannot read config journal", optionalConfigJournal.isPresent());
    return optionalConfigJournal.get();
  }

  public InitializedIndex getInitializedIndex(IndexGeneration indexGeneration) {
    return this.mockedDependencies
        .initializedIndexCatalog
        .getIndex(indexGeneration.getGenerationId())
        .orElseThrow();
  }

  public void waitForIndexInitialization(GenerationId generationId) {
    // Wait up to 1 minute for index to be initialized.

    verify(
            this.mockedDependencies.initializedIndexCatalog,
            timeout(Duration.ofSeconds(60).toMillis()))
        .addIndex(
            argThat(initializedIndex -> initializedIndex.getGenerationId().equals(generationId)));

    while (this.mockedDependencies.initializedIndexCatalog.getIndex(generationId).isEmpty()) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        fail();
      }
    }

    // Refresh cache of index infos.
    this.configManager.updateIndexInfos();
  }
}
