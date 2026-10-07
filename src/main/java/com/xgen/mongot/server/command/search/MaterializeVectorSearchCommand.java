package com.xgen.mongot.server.command.search;

import com.google.common.base.Supplier;
import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.registry.CommandNotFoundException;
import com.xgen.mongot.server.command.search.definition.request.MaterializeVectorSearchCommandDefinition;
import com.xgen.mongot.server.command.search.definition.request.VectorSearchCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.UserFacingException;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.Errors;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;

/**
 * Implements the {@code materializeVectorSearch} command: embeds a {@code $vectorSearch} query's
 * text into a {@code queryVector} and returns the rewritten {@code vectorSearch} wire command,
 * without running a search.
 *
 * <p>On a sharded cluster the router issues this once before scattering the query, so the embedding
 * service is called once per query instead of once per shard. The response is not a search command
 * that can be forwarded verbatim: it carries no {@code collectionUUID}, because each shard resolves
 * its own. The caller is expected to take {@code queryVector} (and {@code model}, the model that
 * produced the vector: the query's model when one is given, otherwise the index's) out of the
 * response and fold them into the stage it scatters.
 *
 * <p>Embedding needs only the applied index definition and the embedding-service credentials, both
 * of which every mongot in the cluster holds. This command therefore does not require the local
 * copy of the index to be queryable: a mongot that is still building or recovering its index can
 * still embed for the shards that are ready.
 */
public class MaterializeVectorSearchCommand implements Command {

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  /** Error label from MongoDB's error_labels.h telling the client the failure may not recur. */
  private static final List<String> RETRYABLE_ERROR_LABELS = List.of("RetryableError");

  private final MaterializeVectorSearchCommandDefinition definition;
  private final IndexCatalog indexCatalog;
  private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;
  private final SearchCommandsRegister.BootstrapperMetadata metadata;
  private final Metrics metrics;

  MaterializeVectorSearchCommand(
      MaterializeVectorSearchCommandDefinition definition,
      IndexCatalog indexCatalog,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      SearchCommandsRegister.BootstrapperMetadata metadata,
      Metrics metrics) {
    this.definition = definition;
    this.indexCatalog = indexCatalog;
    this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
    this.metadata = metadata;
    this.metrics = metrics;
  }

  @Override
  public String name() {
    return MaterializeVectorSearchCommandDefinition.NAME;
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    // Calls the external embedding service, like an auto-embedding vectorSearch.
    return ExecutionPolicy.ASYNC;
  }

  @Override
  public boolean maybeLoadShed() {
    // This command only calls the embedding service and never runs a search, so queueing it
    // would not relieve this node. Shedding it would either push the embedding back onto every
    // shard or fail the query outright, same reasoning as auto-embedding vectorSearch queries.
    return false;
  }

  @Override
  public BsonDocument run() {
    // Rollout gate (see the flag's Javadoc for removal): while off, this mongot behaves as if the
    // command did not exist, which is exactly the signal the router's fallback keys on.
    if (!this.metadata
        .dynamicFeatureFlagRegistry()
        .evaluateClusterInvariant(DynamicFeatureFlags.AUTO_EMBEDDING_MATERIALIZE_VECTOR_SEARCH)) {
      this.metrics.outcome(Outcome.DISABLED).increment();
      return MessageUtils.createError(
          Errors.COMMAND_NOT_FOUND,
          new CommandNotFoundException(MaterializeVectorSearchCommandDefinition.NAME)
              .errorMessage());
    }
    try {
      VectorSearchQuery query = this.definition.getQuery();

      // Same gate as vectorSearch: deadlines are honored only when the query-timeout flag is on.
      // The embedding call itself is bounded by the embedding request timeout, so this only
      // avoids paying for a vector the router will discard because its own deadline has passed.
      if (this.metadata
              .dynamicFeatureFlagRegistry()
              .evaluateClusterInvariant(DynamicFeatureFlags.VECTOR_SEARCH_QUERY_TIMEOUT)
          && isDeadlineExpired(this.definition.deadlineTimestampMs(), System.currentTimeMillis())) {
        this.metrics.outcome(Outcome.DEADLINE_EXPIRED).increment();
        return MessageUtils.createError(
            Errors.MAX_TIME_MS_EXPIRED, "operation exceeded time limit before embedding");
      }

      Optional<IndexGeneration> indexGeneration = resolveIndexGeneration(query.index());
      if (indexGeneration.isEmpty()) {
        this.metrics.outcome(Outcome.INDEX_NOT_FOUND).increment();
        // A vectorSearch against an unknown index answers with no results rather than an error,
        // so this is reported with a code and left to the caller to decide how to proceed.
        return MessageUtils.createError(
            Errors.INDEX_NOT_FOUND,
            "Index '%s' not found for collection '%s.%s'"
                .formatted(query.index(), this.definition.db(), this.definition.collectionName()));
      }
      IndexDefinition appliedDefinition = indexGeneration.get().getDefinition();

      // The same embedding step a vectorSearch runs on a shard, keyed by the namespace the index
      // was resolved for, so the router and the shards cannot disagree about how a query text is
      // embedded. Only the vector is needed here; nothing touches the local Lucene index.
      VectorQueryEmbedder embedder =
          new VectorQueryEmbedder(
              this.indexCatalog,
              this.embeddingServiceManagerSupplier,
              this.definition.db(),
              appliedDefinition.getCollectionUuid(),
              this.definition.viewName());
      VectorSearchCommand.validateIndexNamespace(query, appliedDefinition);
      Optional<VectorQueryEmbedder.EmbedRequestInfo> embedRequestInfo =
          embedder.findEmbedRequestInfo(appliedDefinition, query);
      Vector queryVector = embedder.resolveQueryVector(query, embedRequestInfo);

      this.metrics.outcome(Outcome.SUCCESS).increment();
      return buildResponse(
          query,
          queryVector,
          embedRequestInfo.flatMap(VectorQueryEmbedder.EmbedRequestInfo::modelName));
    } catch (BsonParseException | InvalidQueryException e) {
      return invalidQuery(e);
    } catch (EmbeddingProviderTransientException e) {
      // The condition is about this node or the provider right now (service still starting after
      // a restart, rate limit, timeout), not about the query. The label lets the caller decide to
      // retry elsewhere or fall back to per-shard embedding instead of failing the query.
      this.metrics.outcome(Outcome.EMBEDDING_FAILURE).increment();
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
          "materializeVectorSearch failed to embed the query text (transient)");
      return MessageUtils.createErrorBodyWithLabels(
          e.getMessage() == null ? "failed to embed the query text" : e.getMessage(),
          RETRYABLE_ERROR_LABELS);
    } catch (EmbeddingProviderNonTransientException e) {
      this.metrics.outcome(Outcome.EMBEDDING_FAILURE).increment();
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
          "materializeVectorSearch failed to embed the query text");
      return MessageUtils.createErrorBody(e);
    } catch (Exception e) {
      // Same as vectorSearch: some embedding paths wrap a user error in a RuntimeException.
      if (e instanceof RuntimeException && e.getCause() instanceof InvalidQueryException cause) {
        return invalidQuery(cause);
      }
      this.metrics.outcome(Outcome.INTERNAL_FAILURE).increment();
      if (!UserFacingException.isUserFacing(e)) {
        FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
            "Failed to process a materializeVectorSearch command");
      }
      return MessageUtils.createErrorBody(e);
    }
  }

  private BsonDocument invalidQuery(Exception e) {
    this.metrics.outcome(Outcome.INVALID_QUERY).increment();
    FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
        "materializeVectorSearch query is invalid");
    return MessageUtils.createErrorBody(e);
  }

  static boolean isDeadlineExpired(Optional<Long> deadlineTimestampMs, long nowMs) {
    return deadlineTimestampMs.filter(deadline -> deadline <= nowMs).isPresent();
  }

  /**
   * Resolves the index generation the query refers to by {@code (db, collectionName, indexName)}
   * and, when the request names a view, by view name.
   *
   * <p>Embedding needs only the index definition and the embedding credentials, so a generation is
   * a candidate regardless of its local index status. The router may reach a mongot whose shard
   * holds no data of the collection; that mongot's copy reports {@code DOES_NOT_EXIST} with reason
   * {@code COLLECTION_NOT_FOUND} because local initial sync found no collection. That is locally
   * indistinguishable from a collection dropped everywhere, so filtering such generations out would
   * wrongly answer index-not-found for a live, searchable index. The catalog holds one generation
   * per {@code (db, collectionUUID, indexName)}, so more than one generation can match a collection
   * <em>name</em> only briefly, when a collection was dropped and recreated under the same name and
   * mongot has not yet observed the drop. Shards search the recreated collection, so the newest
   * index id wins; index ids are ObjectIds and therefore order by creation time (to the second,
   * which is far finer than any real drop-and-recreate).
   */
  private Optional<IndexGeneration> resolveIndexGeneration(String indexName) {
    List<IndexGeneration> candidates =
        this.indexCatalog.getIndexes().stream()
            .filter(generation -> matchesRequest(generation.getDefinition(), indexName))
            .toList();
    if (candidates.size() > 1) {
      this.metrics.ambiguousCollectionName.increment();
    }
    return candidates.stream()
        .max(Comparator.comparing(generation -> generation.getDefinition().getIndexId()));
  }

  private boolean matchesRequest(IndexDefinition candidate, String indexName) {
    if (!candidate.getName().equals(indexName)
        || !candidate.getDatabase().equals(this.definition.db())
        || !candidate.getLastObservedCollectionName().equals(this.definition.collectionName())) {
      return false;
    }
    // Same rule as the UUID-keyed catalog lookup: a request that names a view must match the
    // index's view; a request without one accepts any index on the collection.
    Optional<String> requestedView = this.definition.viewName();
    return requestedView.isEmpty()
        || candidate.getView().map(ViewDefinition::getName).equals(requestedView);
  }

  /**
   * The rewritten {@code vectorSearch} wire command (query text replaced by {@code queryVector})
   * under {@code command}, plus the canonical {@code model} that produced the vector so that shards
   * can detect applied-definition skew. {@code collectionUUID} is deliberately absent: each shard
   * supplies its own.
   */
  private BsonDocument buildResponse(
      VectorSearchQuery original, Vector queryVector, Optional<String> model) {
    // The user's query with only the text swapped for the embedded vector. The path and filter
    // stay as the user wrote them: mapping them to the index's internal fields is each shard's job
    // when it runs the query, and the internal names are not part of the query contract.
    VectorSearchQuery rewritten =
        new VectorSearchQuery(
            original.index(),
            original.criteria().withQueryVector(queryVector),
            original.userReturnStoredSource(),
            original.vectorStoredSourceEnabled(),
            original.concurrentOverride(),
            original.indexNamespace());

    BsonDocument command = new BsonDocument();
    command.put(
        VectorSearchCommandDefinition.NAME, new BsonString(this.definition.collectionName()));
    command.put("$db", new BsonString(this.definition.db()));
    this.definition.viewName().ifPresent(view -> command.put("viewName", new BsonString(view)));
    this.definition
        .deadlineTimestampMs()
        .ifPresent(deadline -> command.put("deadlineTimestampMs", new BsonInt64(deadline)));
    command.putAll(rewritten.toBson());

    BsonDocument response = new BsonDocument();
    response.put("ok", new BsonInt32(1));
    response.put("command", command);
    model.ifPresent(m -> response.put("model", new BsonString(m)));
    return response;
  }

  public static class Factory implements CommandFactory {

    private final IndexCatalog indexCatalog;
    private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;
    private final SearchCommandsRegister.BootstrapperMetadata metadata;
    private final Metrics metrics;

    public Factory(
        IndexCatalog indexCatalog,
        Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
        SearchCommandsRegister.BootstrapperMetadata metadata,
        MetricsFactory metricsFactory) {
      this.indexCatalog = indexCatalog;
      this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
      this.metadata = metadata;
      this.metrics = new Metrics(metricsFactory);
    }

    @Override
    public Command create(BsonDocument doc) {
      try (var parser = BsonDocumentParser.fromRoot(doc).allowUnknownFields(true).build()) {
        return create(
            MaterializeVectorSearchCommandDefinition.fromBson(
                parser, this.metadata.featureFlags().isEnabled(Feature.VECTOR_STORED_SOURCE)));
      } catch (BsonParseException e) {
        // we have no way of throwing checked exceptions beyond this method
        // (called directly by opmsg)
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    public Command create(MaterializeVectorSearchCommandDefinition definition) {
      return new MaterializeVectorSearchCommand(
          definition,
          this.indexCatalog,
          this.embeddingServiceManagerSupplier,
          this.metadata,
          this.metrics);
    }
  }

  /** How a call ended; the {@code result} tag on the per-call counter. */
  enum Outcome {
    SUCCESS("success"),
    DISABLED("disabled"),
    INVALID_QUERY("invalidQuery"),
    INDEX_NOT_FOUND("indexNotFound"),
    DEADLINE_EXPIRED("deadlineExpired"),
    EMBEDDING_FAILURE("embeddingFailure"),
    INTERNAL_FAILURE("internalFailure");

    private final String tagValue;

    Outcome(String tagValue) {
      this.tagValue = tagValue;
    }
  }

  /**
   * Outcome counters for the command, created once per factory. Latency and failure-by-exception
   * counts come from the command framework's per-command metrics.
   */
  static class Metrics {
    private final MetricsFactory metricsFactory;
    private final Counter ambiguousCollectionName;

    Metrics(MetricsFactory metricsFactory) {
      this.metricsFactory = metricsFactory;
      this.ambiguousCollectionName =
          metricsFactory.counter("materializeVectorSearchAmbiguousCollectionName");
    }

    /** One call, tagged by how it ended. The registry caches meters, so this lookup is cheap. */
    Counter outcome(Outcome outcome) {
      return this.metricsFactory.counter(
          "materializeVectorSearchCommands", Tags.of("result", outcome.tagValue));
    }
  }
}
