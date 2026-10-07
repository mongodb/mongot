package com.xgen.testing.mongot.server;

import static org.mockito.Mockito.mock;

import com.google.common.base.Supplier;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.rules.TemporaryFolder;

public class Mocks {
  public final MongotCursorManager cursorManager;
  public final MeterRegistry meterRegistry;
  public final ConfigManager configManager;
  public final TemporaryFolder temporaryFolder;
  public final IndexCatalog indexCatalog;
  public final InitializedIndexCatalog initializedIndexCatalog;
  public final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;
  // Dynamic flags the test server evaluates; empty (every flag at its fallback) unless a test opts
  // in.
  public DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry = DynamicFeatureFlagRegistry.empty();

  public Mocks(
      MongotCursorManager cursorManager,
      MeterRegistry meterRegistry,
      ConfigManager configManager,
      TemporaryFolder temporaryFolder,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog) {
    this.cursorManager = cursorManager;
    this.meterRegistry = meterRegistry;
    this.configManager = configManager;
    this.temporaryFolder = temporaryFolder;
    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.embeddingServiceManagerSupplier = () -> mock(EmbeddingServiceManager.class);
  }

  public Mocks withDynamicFeatureFlagRegistry(DynamicFeatureFlagRegistry registry) {
    this.dynamicFeatureFlagRegistry = registry;
    return this;
  }
}
