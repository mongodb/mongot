package com.xgen.mongot.index.lucene.synonym;

import com.google.common.collect.Maps;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.index.synonym.SynonymDetailedStatus;
import com.xgen.mongot.index.synonym.SynonymMapping;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.util.CollectionUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneSynonymRegistry implements SynonymRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(LuceneSynonymRegistry.class);

  private final AnalyzerRegistry analyzerRegistry;
  private final ConcurrentMap<String, StatusMappingPair> synonymMappings;
  private final Optional<Integer> maxDocsPerSynonymCollection;

  static class StatusMappingPair {
    private final Optional<SynonymMapping> synonymMapping;
    private final SynonymStatus synonymStatus;

    private final Optional<String> message;

    StatusMappingPair(SynonymMapping synonymMapping, SynonymStatus synonymStatus) {
      this(Optional.of(synonymMapping), synonymStatus);
    }

    StatusMappingPair(Optional<SynonymMapping> synonymMapping, SynonymStatus synonymStatus) {
      this.synonymMapping = synonymMapping;
      this.synonymStatus = synonymStatus;
      this.message = Optional.empty();
    }

    StatusMappingPair(
        Optional<SynonymMapping> synonymMapping,
        SynonymStatus synonymStatus,
        Optional<String> message) {
      this.synonymMapping = synonymMapping;
      this.synonymStatus = synonymStatus;
      this.message = message;
    }

    public Optional<SynonymMapping> getSynonymMapping() {
      return this.synonymMapping;
    }

    public SynonymStatus getSynonymStatus() {
      return this.synonymStatus;
    }

    public SynonymDetailedStatus getDetailedStatus() {
      return new SynonymDetailedStatus(this.synonymStatus, this.message);
    }

    public Optional<String> getMessage() {
      return this.message;
    }

    StatusMappingPair withNewStatus(SynonymStatus synonymStatus) {
      return new StatusMappingPair(this.synonymMapping, synonymStatus);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StatusMappingPair that = (StatusMappingPair) o;
      return this.synonymMapping.equals(that.synonymMapping)
          && this.synonymStatus == that.synonymStatus
          && this.message.equals(that.message);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.synonymMapping, this.synonymStatus, this.message);
    }
  }

  private LuceneSynonymRegistry(
      AnalyzerRegistry analyzerRegistry,
      ConcurrentMap<String, StatusMappingPair> synonymMappings,
      Optional<Integer> maxDocsPerSynonymCollection) {

    this.analyzerRegistry = analyzerRegistry;
    this.synonymMappings = synonymMappings;
    this.maxDocsPerSynonymCollection = maxDocsPerSynonymCollection;
  }

  /**
   * Instantiates a {@link LuceneSynonymRegistry}. At instantiation, each {@link SynonymMapping}
   * contains the {@code baseAnalyzerName} configured in its {@link SynonymMappingDefinition}, and
   * the unmodified analyzer of that name.
   *
   * <p>At instantiation, the analyzer in a {@link SynonymMapping} is the same analyzer as the one
   * in the analyzerRegistry that is registered to {@code baseAnalyzerName}.
   */
  public static LuceneSynonymRegistry create(
      AnalyzerRegistry analyzerRegistry,
      Map<String, SynonymMappingDefinition> synonymDefinitions,
      Optional<Integer> maxDocumentsPerSynonymCollection) {
    Function<Map.Entry<?, SynonymMappingDefinition>, StatusMappingPair> synonymMappingInitializer =
        synonymDefinitionMapEntry ->
            new StatusMappingPair(
                new SynonymMapping(
                    analyzerRegistry.getAnalyzer(synonymDefinitionMapEntry.getValue().analyzer()),
                    synonymDefinitionMapEntry.getValue().analyzer()),
                SynonymStatus.SYNC_ENQUEUED);

    return new LuceneSynonymRegistry(
        analyzerRegistry,
        synonymDefinitions.entrySet().stream()
            .collect(
                CollectionUtils.toConcurrentMapUnsafe(
                    Map.Entry::getKey, synonymMappingInitializer)),
        maxDocumentsPerSynonymCollection);
  }

  @Override
  public SynonymMapping.Builder mappingBuilder(SynonymMappingDefinition definition) {
    return LuceneSynonymMapBuilder.builder(
        this.analyzerRegistry.getAnalyzer(definition.analyzer()), definition.analyzer());
  }

  /**
   * Get the {@link SynonymMapping} with name {@code synonymMappingName}.
   *
   * @throws SynonymMappingException of type {@link SynonymMappingException.Type#UNKNOWN_MAPPING} if
   *     there is no mapping with name {@code synonymMappingName}.
   */
  @Override
  public SynonymMapping get(String synonymMappingName) throws SynonymMappingException {
    Optional<StatusMappingPair> optionalStatusMappingPair =
        Optional.ofNullable(this.synonymMappings.get(synonymMappingName));

    // If we don't have either a mapping or a status, let a querying user know that this synonym
    // mapping does not exist.
    if (optionalStatusMappingPair.isEmpty()) {
      throw SynonymMappingException.unknownMappingName(synonymMappingName);
    }

    StatusMappingPair mappingPair = optionalStatusMappingPair.get();
    if (!mappingPair.getSynonymStatus().isReady()) {
      throw SynonymMappingException.mappingNotReady(
          synonymMappingName, mappingPair.getSynonymStatus());
    }

    Optional<SynonymMapping> synonymMapping = mappingPair.getSynonymMapping();
    if (synonymMapping.isEmpty()) {
      throw new AssertionError("should never have ready status without mapping");
    }

    return synonymMapping.get();
  }

  /**
   * Insert a {@link SynonymMapping} into this registry, clobbering any existing mapping with that
   * name. Marks the {@link SynonymStatus} for this mapping as {@link SynonymStatus#READY}.
   */
  @Override
  public void update(String name, SynonymMapping synonymMapping) {
    this.synonymMappings.put(name, new StatusMappingPair(synonymMapping, SynonymStatus.READY));
  }

  /**
   * Marks that a change was observed that is relevant to {@link SynonymMapping}. Does not modify
   * artifacts associated with a {@link SynonymMapping}, only changes the {@link SynonymStatus}.
   * Intended to notify users that this {@link SynonymMapping} is being updated, and may soon
   * change.
   *
   * <p>When {@link SynonymStatus#READY}, changes status to {@link SynonymStatus#READY_UPDATING}.
   *
   * <p>When {@link SynonymStatus#INVALID}, changes status to {@link SynonymStatus#SYNC_ENQUEUED}.
   */
  @Override
  public void observeChange(String name) {
    this.synonymMappings.compute(
        name,
        (ignored, statusMappingPair) -> {
          if (statusMappingPair == null) {
            throw new AssertionError(
                String.format("expected status for mapping with name %s", name));
          }

          return switch (statusMappingPair.getSynonymStatus()) {
            case FAILED, SYNC_ENQUEUED, INITIAL_SYNC, READY_UPDATING -> {
              LOG.atWarn()
                  .addKeyValue("stateName", name)
                  .log("should not be able to observe a change when in state {}", name);
              yield statusMappingPair;
            }
            case INVALID -> statusMappingPair.withNewStatus(SynonymStatus.SYNC_ENQUEUED);
            case READY -> statusMappingPair.withNewStatus(SynonymStatus.READY_UPDATING);
          };
        });
  }

  /**
   * Marks that work has begun to update a {@link SynonymMapping}. Does not modify artifacts
   * associated with a {@link SynonymMapping}, only changes the {@link SynonymStatus}. Intended to
   * notify users that this {@link SynonymMapping} is being updated, and may soon be replaced with a
   * more recent artifact or invalidated.
   *
   * <p>When {@link SynonymStatus#INVALID} or {@link SynonymStatus#SYNC_ENQUEUED}, changes status to
   * {@link SynonymStatus#INITIAL_SYNC}.
   *
   * <p>When {@link SynonymStatus#READY}, changes status to {@link SynonymStatus#READY_UPDATING}.
   */
  @Override
  public void beginUpdate(String name) {
    this.synonymMappings.compute(
        name,
        (ignored, statusMappingPair) -> {
          if (statusMappingPair == null) {
            throw new AssertionError(
                String.format("expected status for mapping with name %s", name));
          }

          return switch (statusMappingPair.getSynonymStatus()) {
            case FAILED -> {
              LOG.atWarn()
                  .addKeyValue("stateName", name)
                  .log("should not begin update on failed synonym mapping {}", name);
              yield statusMappingPair;
            }
            case INVALID, SYNC_ENQUEUED, INITIAL_SYNC ->
                statusMappingPair.withNewStatus(SynonymStatus.INITIAL_SYNC);
            case READY_UPDATING, READY ->
                statusMappingPair.withNewStatus(SynonymStatus.READY_UPDATING);
          };
        });
  }

  /**
   * Sets the {@link SynonymMapping} associated with this {@link SynonymMappingDefinition} to an
   * empty {@link SynonymMapping}. Marks the {@link SynonymStatus} for this mapping as {@link
   * SynonymStatus#READY}.
   *
   * <p>Future attempts to get a {@link SynonymMapping} for this {@link SynonymMappingDefinition}
   * should find an associated mapping configured to apply a valid, empty set of synonyms.
   *
   * <p>This may be used, for example, when a synonym source collection backing a {@link
   * SynonymMapping} is dropped.
   *
   * <p>{@code clear} may be called regardless of whether or not this {@link SynonymRegistry}
   * contains an existing, valid {@link SynonymMapping} for this {@link SynonymMappingDefinition}.
   */
  @Override
  public void clear(SynonymMappingDefinition definition) {
    update(
        definition.name(),
        new SynonymMapping(
            this.analyzerRegistry.getAnalyzer(definition.analyzer()), definition.analyzer()));
  }

  /**
   * Removes a {@link SynonymMapping} from this registry, releasing artifacts associated with it.
   * Marks the {@link SynonymStatus} for this mapping as {@link SynonymStatus#INVALID}.
   *
   * <p>Future attempts to get a {@link SynonymMapping} named {@code name} should not find an
   * associated mapping. This may be used, for example, when an invalid document is present in a
   * synonym source collection.
   */
  @Override
  public void invalidate(String name, String message) {
    this.synonymMappings.put(
        name, new StatusMappingPair(Optional.empty(), SynonymStatus.INVALID, Optional.of(message)));
  }

  /**
   * Removes a {@link SynonymMapping} from this registry, releasing artifacts associated with it.
   * Marks the {@link SynonymStatus} for this mapping as {@link SynonymStatus#FAILED}.
   *
   * <p>Future attempts to get a {@link SynonymMapping} named {@code name} will not find an
   * associated mapping. This may be used when a mapping is shutting down - and is not expected
   * behavior otherwise.
   */
  @Override
  public void fail(String name, @Nullable String message) {
    this.synonymMappings.put(
        name,
        new StatusMappingPair(
            Optional.empty(), SynonymStatus.FAILED, Optional.ofNullable(message)));
  }

  /**
   * The {@link SynonymStatus}es for each {@link SynonymMapping} that exists in the index. {@link
   * SynonymMapping}s have a {@link SynonymStatus} in this map after they are first created - and
   * will continue to have a status for the lifetime of this {@link SynonymRegistry}.
   */
  @Override
  public Map<String, SynonymStatus> getStatuses() {
    return Collections.unmodifiableMap(
        Maps.transformValues(this.synonymMappings, StatusMappingPair::getSynonymStatus));
  }

  @Override
  public Map<String, SynonymDetailedStatus> getDetailedStatuses() {
    return Collections.unmodifiableMap(
        Maps.transformValues(this.synonymMappings, StatusMappingPair::getDetailedStatus));
  }

  @Override
  public Optional<Integer> getMaxDocsPerSynonymCollection() {
    return this.maxDocsPerSynonymCollection;
  }
}
