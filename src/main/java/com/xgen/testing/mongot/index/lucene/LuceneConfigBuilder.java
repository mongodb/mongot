package com.xgen.testing.mongot.index.lucene;

import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.testing.TestUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import org.junit.rules.TemporaryFolder;

public class LuceneConfigBuilder {

  private Optional<Path> dataPath = Optional.empty();
  private Optional<Duration> refreshInterval = Optional.empty();
  private Optional<Integer> refreshExecutorThreads = Optional.empty();
  private Optional<Integer> numMaxMergeThreads = Optional.empty();
  private Optional<Integer> numMaxMerges = Optional.empty();
  private Optional<Double> ramBufferSizeMb = Optional.empty();
  private Optional<Double> nrtCacheSizeMb = Optional.empty();
  private Optional<Boolean> nrtCacheEnabled = Optional.empty();
  private Optional<Double> nrtMergeSizeMb = Optional.empty();
  private Optional<Bytes> maxMergedSegmentSize = Optional.empty();
  private Optional<Integer> fieldLimit = Optional.empty();
  private Optional<Integer> docsLimit = Optional.empty();
  private Optional<Integer> maxSynonymMappingsPerIndex = Optional.empty();
  private Optional<Integer> maxDocumentsPerSynonymCollection = Optional.empty();
  private Optional<Boolean> disableMaxClauses = Optional.empty();
  private Optional<Integer> maxClauseLimit = Optional.empty();
  private Optional<Boolean> enableConcurrentSearch = Optional.empty();
  private Optional<Integer> concurrentSearchExecutorThreads = Optional.empty();
  private Optional<Integer> concurrentSearchExecutorQueueSize = Optional.empty();
  private Optional<Integer> concurrentVectorRescoringExecutorThreads = Optional.empty();
  private Optional<Integer> concurrentVectorRescoringExecutorQueueSize = Optional.empty();
  private Optional<LuceneConfig.VectorMergePolicyConfig> vectorMergePolicyConfig = Optional.empty();
  private Optional<Boolean> enableTextOperatorNewSynonymsSyntax = Optional.empty();
  private Optional<Integer> tokenFacetingCardinalityLimit = Optional.empty();
  private Optional<Integer> aggregationMaxBuckets = Optional.empty();
  private Optional<Double> deletesPctAllowed = Optional.empty();
  private Optional<Double> forceMergeDeletesPctAllowed = Optional.empty();
  private Optional<Double> floorSegmentMB = Optional.empty();
  private Optional<HysteresisConfig> mergePolicyDiskUtilizationConfig = Optional.empty();
  private Optional<Long> cancelMergePerThreadTimeoutMs = Optional.empty();
  private Optional<Long> cancelAllMergesPerThreadTimeoutMs = Optional.empty();

  public static LuceneConfigBuilder builder() {
    return new LuceneConfigBuilder();
  }

  public LuceneConfigBuilder dataPath(Path dataPath) {
    this.dataPath = Optional.of(dataPath);
    return this;
  }

  /** Creates a temporary folder to set as the dataPath. */
  public LuceneConfigBuilder tempDataPath() {
    try {
      TemporaryFolder folder = TestUtils.getTempFolder();
      this.dataPath = Optional.of(folder.getRoot().toPath());
      return this;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public LuceneConfigBuilder refreshInterval(Duration refreshInterval) {
    this.refreshInterval = Optional.of(refreshInterval);
    return this;
  }

  public LuceneConfigBuilder refreshExecutorThreads(int refreshExecutorThreads) {
    this.refreshExecutorThreads = Optional.of(refreshExecutorThreads);
    return this;
  }

  public LuceneConfigBuilder numMaxMergeThreads(int numMaxMergeThreads) {
    this.numMaxMergeThreads = Optional.of(numMaxMergeThreads);
    return this;
  }

  public LuceneConfigBuilder numMaxMerges(int numMaxMerges) {
    this.numMaxMerges = Optional.of(numMaxMerges);
    return this;
  }

  public LuceneConfigBuilder ramBufferSizeMb(double ramBufferSizeMb) {
    this.ramBufferSizeMb = Optional.of(ramBufferSizeMb);
    return this;
  }

  public LuceneConfigBuilder nrtCacheEnabled(boolean nrtCacheEnabled) {
    this.nrtCacheEnabled = Optional.of(nrtCacheEnabled);
    return this;
  }

  public LuceneConfigBuilder nrtCacheSizeMb(double nrtCacheSizeMb) {
    this.nrtCacheSizeMb = Optional.of(nrtCacheSizeMb);
    return this;
  }

  public LuceneConfigBuilder nrtMergeSizeMb(double nrtMergeSizeMb) {
    this.nrtMergeSizeMb = Optional.of(nrtMergeSizeMb);
    return this;
  }

  public LuceneConfigBuilder maxMergedSegmentSize(Bytes maxMergedSegmentSize) {
    this.maxMergedSegmentSize = Optional.of(maxMergedSegmentSize);
    return this;
  }

  public LuceneConfigBuilder fieldLimit(int fieldLimit) {
    this.fieldLimit = Optional.of(fieldLimit);
    return this;
  }

  public LuceneConfigBuilder docsLimit(int docsLimit) {
    this.docsLimit = Optional.of(docsLimit);
    return this;
  }

  public LuceneConfigBuilder maxSynonymMappingsPerIndex(int maxSynonymMappingsPerIndex) {
    this.maxSynonymMappingsPerIndex = Optional.of(maxSynonymMappingsPerIndex);
    return this;
  }

  public LuceneConfigBuilder maxDocumentsPerSynonymCollection(
      int maxDocumentsPerSynonymCollection) {
    this.maxDocumentsPerSynonymCollection = Optional.of(maxDocumentsPerSynonymCollection);
    return this;
  }

  public LuceneConfigBuilder disableMaxClauses(boolean value) {
    this.disableMaxClauses = Optional.of(value);
    return this;
  }

  public LuceneConfigBuilder maxClauseLimit(int value) {
    this.maxClauseLimit = Optional.of(value);
    return this;
  }

  public LuceneConfigBuilder enableConcurrentSearch(boolean value) {
    this.enableConcurrentSearch = Optional.of(value);
    return this;
  }

  public LuceneConfigBuilder concurrentSearchExecutorThreads(int value) {
    this.concurrentSearchExecutorThreads = Optional.of(value);
    return this;
  }

  public LuceneConfigBuilder concurrentSearchExecutorQueueSize(int value) {
    this.concurrentSearchExecutorQueueSize = Optional.of(value);
    return this;
  }

  public LuceneConfigBuilder concurrentVectorRescoringExecutorThreads(int value) {
    this.concurrentVectorRescoringExecutorThreads = Optional.of(value);
    return this;
  }

  public LuceneConfigBuilder concurrentVectorRescoringExecutorQueueSize(int value) {
    this.concurrentVectorRescoringExecutorQueueSize = Optional.of(value);
    return this;
  }

  public LuceneConfigBuilder vectorMergePolicyConfig(
      LuceneConfig.VectorMergePolicyConfig vectorMergePolicyConfig) {
    this.vectorMergePolicyConfig = Optional.of(vectorMergePolicyConfig);
    return this;
  }

  public LuceneConfigBuilder enableTextOperatorNewSynonymsSyntax(
      boolean enableTextOperatorNewSynonymsSyntax) {
    this.enableTextOperatorNewSynonymsSyntax = Optional.of(enableTextOperatorNewSynonymsSyntax);
    return this;
  }

  public LuceneConfigBuilder tokenFacetingCardinalityLimit(int tokenFacetingCardinalityLimit) {
    this.tokenFacetingCardinalityLimit = Optional.of(tokenFacetingCardinalityLimit);
    return this;
  }

  public LuceneConfigBuilder aggregationMaxBuckets(int aggregationMaxBuckets) {
    this.aggregationMaxBuckets = Optional.of(aggregationMaxBuckets);
    return this;
  }

  public LuceneConfigBuilder deletesPctAllowed(double deletesPctAllowed) {
    this.deletesPctAllowed = Optional.of(deletesPctAllowed);
    return this;
  }

  public LuceneConfigBuilder forceMergeDeletesPctAllowed(double forceMergeDeletesPctAllowed) {
    this.forceMergeDeletesPctAllowed = Optional.of(forceMergeDeletesPctAllowed);
    return this;
  }

  public LuceneConfigBuilder floorSegmentMB(double floorSegmentMB) {
    this.floorSegmentMB = Optional.of(floorSegmentMB);
    return this;
  }

  public LuceneConfigBuilder mergePolicyDiskUtilizationConfig(
      HysteresisConfig mergePolicyDiskUtilizationConfig) {
    this.mergePolicyDiskUtilizationConfig = Optional.of(mergePolicyDiskUtilizationConfig);
    return this;
  }

  public LuceneConfigBuilder cancelMergePerThreadTimeoutMs(long cancelMergePerThreadTimeoutMs) {
    this.cancelMergePerThreadTimeoutMs = Optional.of(cancelMergePerThreadTimeoutMs);
    return this;
  }

  public LuceneConfigBuilder cancelAllMergesPerThreadTimeoutMs(
      long cancelAllMergesPerThreadTimeoutMs) {
    this.cancelAllMergesPerThreadTimeoutMs = Optional.of(cancelAllMergesPerThreadTimeoutMs);
    return this;
  }

  public LuceneConfig build() {
    Check.isPresent(this.dataPath, "dataPath");

    return LuceneConfig.create(
        this.dataPath.get(),
        this.refreshInterval,
        this.refreshExecutorThreads,
        this.numMaxMergeThreads,
        this.numMaxMerges,
        this.ramBufferSizeMb,
        this.nrtCacheEnabled,
        this.nrtCacheSizeMb,
        this.nrtMergeSizeMb,
        this.maxMergedSegmentSize,
        this.fieldLimit,
        this.docsLimit,
        this.maxSynonymMappingsPerIndex,
        this.maxDocumentsPerSynonymCollection,
        this.disableMaxClauses,
        this.maxClauseLimit,
        this.enableConcurrentSearch,
        this.concurrentSearchExecutorThreads,
        this.concurrentSearchExecutorQueueSize,
        this.concurrentVectorRescoringExecutorThreads,
        this.concurrentVectorRescoringExecutorQueueSize,
        this.vectorMergePolicyConfig,
        this.enableTextOperatorNewSynonymsSyntax,
        this.tokenFacetingCardinalityLimit,
        this.aggregationMaxBuckets,
        this.deletesPctAllowed,
        this.forceMergeDeletesPctAllowed,
        this.floorSegmentMB,
        this.mergePolicyDiskUtilizationConfig,
        this.cancelMergePerThreadTimeoutMs,
        this.cancelAllMergesPerThreadTimeoutMs);
  }
}
