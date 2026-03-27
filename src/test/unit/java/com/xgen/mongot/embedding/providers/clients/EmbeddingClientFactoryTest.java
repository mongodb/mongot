package com.xgen.mongot.embedding.providers.clients;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.config.util.DeploymentEnvironment;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.congestion.AimdCongestionControl;
import com.xgen.mongot.embedding.providers.congestion.DynamicSemaphore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public class EmbeddingClientFactoryTest {

  private static final EmbeddingServiceConfig.ErrorHandlingConfig RETRY_CONFIG =
      new EmbeddingServiceConfig.ErrorHandlingConfig(3, 10L, 10L, 0.1);

  private static EmbeddingModelConfig voyageModelWithQueryAndCollectionScan() {
    EmbeddingServiceConfig.VoyageEmbeddingCredentials creds =
        new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
            "token", "2024-10-15T22:32:20.925Z");
    EmbeddingServiceConfig.WorkloadParams queryParams =
        new EmbeddingServiceConfig.WorkloadParams(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(creds));
    EmbeddingServiceConfig.WorkloadParams collectionScanParams =
        new EmbeddingServiceConfig.WorkloadParams(
            Optional.empty(), Optional.empty(), Optional.of(creds), Optional.empty());
    EmbeddingServiceConfig.EmbeddingConfig config =
        new EmbeddingServiceConfig.EmbeddingConfig(
            Optional.empty(),
            new EmbeddingServiceConfig.VoyageModelConfig(
                Optional.of(1024),
                Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                Optional.of(100),
                Optional.of(120_000)),
            RETRY_CONFIG,
            creds,
            Optional.of(queryParams),
            Optional.of(collectionScanParams),
            Optional.empty(),
            Optional.empty(),
            true,
            Optional.empty(),
            EmbeddingServiceConfig.EmbeddingConfig.DEFAULT_USE_FLEX_TIER,
            Optional.empty());
    return EmbeddingModelConfig.create(
        "voyage-3-large", EmbeddingServiceConfig.EmbeddingProvider.VOYAGE, config);
  }

  private static EmbeddingModelConfig voyageModelFlexTierDisabled() {
    EmbeddingServiceConfig.VoyageEmbeddingCredentials creds =
        new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
            "token", "2024-10-15T22:32:20.925Z");
    EmbeddingServiceConfig.WorkloadParams collectionScanParams =
        new EmbeddingServiceConfig.WorkloadParams(
            Optional.empty(), Optional.empty(), Optional.of(creds), Optional.empty());
    EmbeddingServiceConfig.EmbeddingConfig config =
        new EmbeddingServiceConfig.EmbeddingConfig(
            Optional.empty(),
            new EmbeddingServiceConfig.VoyageModelConfig(
                Optional.of(1024),
                Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
                Optional.of(100),
                Optional.of(120_000)),
            RETRY_CONFIG,
            creds,
            Optional.empty(),
            Optional.of(collectionScanParams),
            Optional.empty(),
            Optional.empty(),
            true,
            Optional.empty(),
            false,
            Optional.empty());
    return EmbeddingModelConfig.create(
        "voyage-3-large", EmbeddingServiceConfig.EmbeddingProvider.VOYAGE, config);
  }

  private static DynamicSemaphore testSemaphore() {
    return new DynamicSemaphore(new AimdCongestionControl());
  }

  @Test
  public void atlasFlexTierIncludesTier_setsCongestionSemaphoreOnVoyageClient() throws Exception {
    EmbeddingModelConfig model = voyageModelWithQueryAndCollectionScan();
    EmbeddingClientFactory factory =
        new EmbeddingClientFactory(
            new SimpleMeterRegistry(),
            DeploymentEnvironment.ATLAS,
            Optional.of(Set.of(EmbeddingServiceConfig.ServiceTier.QUERY)));
    DynamicSemaphore sem = testSemaphore();

    ClientInterface client =
        factory.createEmbeddingClient(
            model, EmbeddingServiceConfig.ServiceTier.QUERY, model.query(), Optional.of(sem));

    assertNotNull(congestionSemaphoreField(client));
  }

  @Test
  public void atlasFlexTierExcludesTier_doesNotSetCongestionSemaphore() throws Exception {
    EmbeddingModelConfig model = voyageModelWithQueryAndCollectionScan();
    EmbeddingClientFactory factory =
        new EmbeddingClientFactory(
            new SimpleMeterRegistry(),
            DeploymentEnvironment.ATLAS,
            Optional.of(Set.of(EmbeddingServiceConfig.ServiceTier.QUERY)));
    DynamicSemaphore sem = testSemaphore();

    ClientInterface client =
        factory.createEmbeddingClient(
            model,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            model.collectionScan(),
            Optional.of(sem));

    assertNull(congestionSemaphoreField(client));
  }

  @Test
  public void embeddingConfigUseFlexTierFalse_disablesFlexTierOnVoyageClient() throws Exception {
    EmbeddingModelConfig model = voyageModelFlexTierDisabled();
    EmbeddingClientFactory factory =
        new EmbeddingClientFactory(
            new SimpleMeterRegistry(),
            DeploymentEnvironment.ATLAS,
            Optional.of(Set.of(EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN)));
    DynamicSemaphore sem = testSemaphore();

    ClientInterface client =
        factory.createEmbeddingClient(
            model,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            model.collectionScan(),
            Optional.of(sem));

    assertFalse(useFlexTierField(client));
    assertNull(congestionSemaphoreField(client));
  }

  @Test
  public void optionalSemaphoreEmpty_doesNotSetCongestionSemaphore() throws Exception {
    EmbeddingModelConfig model = voyageModelWithQueryAndCollectionScan();
    EmbeddingClientFactory factory =
        new EmbeddingClientFactory(
            new SimpleMeterRegistry(),
            DeploymentEnvironment.ATLAS,
            Optional.of(Set.of(EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN)));

    ClientInterface client =
        factory.createEmbeddingClient(
            model,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            model.collectionScan(),
            Optional.empty());

    assertNull(congestionSemaphoreField(client));
  }

  @Test
  public void atlasFlexTier_collectionScanUsesFlexTierWhenCongestionSemaphoreProvided()
      throws Exception {
    EmbeddingModelConfig model = voyageModelWithQueryAndCollectionScan();
    EmbeddingClientFactory factory =
        new EmbeddingClientFactory(
            new SimpleMeterRegistry(),
            DeploymentEnvironment.ATLAS,
            Optional.of(Set.of(EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN)));
    DynamicSemaphore sem = testSemaphore();

    ClientInterface client =
        factory.createEmbeddingClient(
            model,
            EmbeddingServiceConfig.ServiceTier.COLLECTION_SCAN,
            model.collectionScan(),
            Optional.of(sem));

    assertTrue(useFlexTierField(client));
    assertNotNull(congestionSemaphoreField(client));
  }

  private static boolean useFlexTierField(ClientInterface client) throws Exception {
    Field f = VoyageClient.class.getDeclaredField("useFlexTier");
    f.setAccessible(true);
    return f.getBoolean(client);
  }

  private static Object congestionSemaphoreField(ClientInterface client) throws Exception {
    Field f = VoyageClient.class.getDeclaredField("congestionSemaphore");
    f.setAccessible(true);
    return f.get(client);
  }
}
