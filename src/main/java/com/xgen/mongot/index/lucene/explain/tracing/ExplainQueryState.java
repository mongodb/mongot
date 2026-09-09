package com.xgen.mongot.index.lucene.explain.tracing;

import static com.xgen.mongot.index.definition.IndexDefinition.Fields.NUM_PARTITIONS;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.MustBeClosed;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.util.Check;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.ImplicitContextKeyed;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import javax.annotation.WillCloseWhenClosed;
import oshi.SystemInfo;

/**
 * <code>ExplainQueryState</code> is an object held in {@link Context} during <code>explain()</code>
 * requests that is used to record execution data to report as part of the response.
 */
@VisibleForTesting
public class ExplainQueryState implements ImplicitContextKeyed {
  private static final ContextKey<ExplainQueryState> KEY = ContextKey.named("mongot.explain");

  /**
   * ContextKey used to store the current partition's QueryInfo key in the thread-local Context. It
   * is possible that queries are executred concurrently over index partitions, so keeping the
   * current partition's key in a thread-local Context allows us to avoid using locks.
   */
  private static final ContextKey<ContextKey<Explain.QueryInfo>> CURRENT_PARTITION_KEY =
      ContextKey.named("mongot.explain.currentPartitionKey");

  private final Explain.QueryInfo rootQueryInfo;
  private final SystemInfo systemInfo;

  private final Map<Integer, KeyAndContext> indexPartitionQueryContexts;
  private final int numPartitions;

  /**
   * Fetches <code>ExplainQueryState</code> if it is set on the current {@link Context}.
   *
   * @return The current <code>ExplainQueryState</code> or <code>empty()</code>.
   */
  static Optional<ExplainQueryState> getFromContext() {
    return Optional.ofNullable(Context.current().get(KEY));
  }

  @Override
  public Context storeInContext(Context context) {
    return context.with(KEY, this);
  }

  /**
   * Create a new <code>ExplainQueryState</code>. This automatically records resource usage for the
   * current thread; use <code>wrap()</code> to ensure code executed on another thread is also
   * recorded.
   *
   * <p>Install this in the current {@link Context} to ensure data is recorded:
   *
   * <pre>{@code
   * try (var scope = Context.current().with(new ExplainQueryState()).getFromContext()) {
   *   search.run();
   * }
   * }</pre>
   */
  public ExplainQueryState(Explain.QueryInfo queryInfo, int numPartitions) {
    this(queryInfo, numPartitions, new SystemInfo());
  }

  public ExplainQueryState(SystemInfo systemInfo) {
    this(new Explain.QueryInfo(Explain.Verbosity.QUERY_PLANNER), 0, systemInfo);
  }

  ExplainQueryState(Explain.QueryInfo queryInfo, int numPartitions, SystemInfo systemInfo) {
    this.rootQueryInfo = queryInfo;
    this.systemInfo = systemInfo;

    this.numPartitions = numPartitions;
    this.indexPartitionQueryContexts = new HashMap<>();

    createIndexPartitionQueryContexts(numPartitions);
    // Initialize the explainer for ResourceUsageCollector immediately. This explainer is
    // non-optional.
    getResourceUsageExplainer();
  }

  public Explain.QueryInfo getQueryInfo() {
    // Get the current partition key from the thread-local Context (not a shared field)
    Optional<ContextKey<Explain.QueryInfo>> currentPartitionKey =
        Optional.ofNullable(Context.current().get(CURRENT_PARTITION_KEY));

    if (currentPartitionKey.isPresent()) {
      Optional<Explain.QueryInfo> indexPartitionQueryInfo =
          Optional.ofNullable(Context.current().get(currentPartitionKey.get()));
      if (indexPartitionQueryInfo.isPresent()) {
        return indexPartitionQueryInfo.get();
      }
    }

    // Return the root queryInfo if no index partition queryInfo is found
    return getRootQueryInfo();
  }

  public int getNumPartitions() {
    return this.numPartitions;
  }

  public Explain.QueryInfo getRootQueryInfo() {
    return this.rootQueryInfo;
  }

  /**
   * QueryInfo for each index partition that was allocated a sub-context. Empty when the index is
   * not partitioned (the default of one partition), since those queries record on the root.
   *
   * <p>Does not create FeatureExplainers; callers that want existing ones look them up without a
   * factory.
   */
  public List<Explain.QueryInfo> getIndexPartitionQueryInfos() {
    List<Explain.QueryInfo> infos = new ArrayList<>();
    for (int i = 0; i < this.numPartitions; i++) {
      Optional.ofNullable(this.indexPartitionQueryContexts.get(i))
          .map(keyAndContext -> keyAndContext.context.get(keyAndContext.key))
          .ifPresent(infos::add);
    }
    return infos;
  }

  private void createIndexPartitionQueryContexts(int numPartitions) {
    if (numPartitions == NUM_PARTITIONS.getDefaultValue()) {
      // Do not create index partition contexts if default is specified
      return;
    }

    for (int i = 0; i < numPartitions; i++) {
      ContextKey<Explain.QueryInfo> key = ContextKey.named(String.valueOf(i));
      Context indexPartitionContext =
          Context.current()
              .with(key, new Explain.QueryInfo(this.rootQueryInfo.getVerbosity()))
              // Since the child contexts are being created during the instantiation of the
              // ExplainQueryState, they will not contain an implicit reference to the enclosing
              // ExplainQueryState. As a workaround, we explicitly store the ExplainQueryState.
              .with(this);

      this.indexPartitionQueryContexts.put(i, new KeyAndContext(key, indexPartitionContext));
    }
  }

  @SuppressWarnings("MustBeClosedChecker")
  public Scope enterIndexPartitionQueryContext(int indexPartitionId) {
    Optional<KeyAndContext> maybeKeyAndContext =
        Optional.ofNullable(this.indexPartitionQueryContexts.get(indexPartitionId));
    KeyAndContext keyAndContext = Check.isPresent(maybeKeyAndContext, "keyAndContext");

    // Store the partition key in the Context itself (thread-local), not a shared field.
    // When the returned Scope is closed, the previous Context is restored automatically,
    // which means the partition key is effectively "cleared" without explicit action.
    return keyAndContext.context.with(CURRENT_PARTITION_KEY, keyAndContext.key).makeCurrent();
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public SearchExplainInformation collect() {
    var builder = SearchExplainInformationBuilder.newBuilder();
    for (var featureExplainer : this.rootQueryInfo.getFeatureExplainers()) {
      FeatureExplainerEmitter.create(featureExplainer)
          .aggregate()
          .emitExplanation(this.rootQueryInfo.getVerbosity(), builder);
    }

    for (var keyAndContext : this.indexPartitionQueryContexts.values()) {
      var indexPartitionBuilder = SearchExplainInformationBuilder.newBuilder();
      Optional<Explain.QueryInfo> indexPartitionQueryInfo =
          Optional.ofNullable(keyAndContext.context.get(keyAndContext.key));

      if (indexPartitionQueryInfo.isPresent()) {
        for (var featureExplainer : indexPartitionQueryInfo.get().getFeatureExplainers()) {
          FeatureExplainerEmitter.create(featureExplainer)
              .aggregate()
              .emitExplanation(indexPartitionQueryInfo.get().getVerbosity(), indexPartitionBuilder);
        }
      }

      builder.addIndexPartitionExplainInformation(indexPartitionBuilder.build());
    }

    return builder.build();
  }

  /**
   * Wraps an input {@link Executor} so that all passed closures will report their resource usage
   * back to this object on completion.
   *
   * @return A resource usage recording <code>Executor</code>
   */
  public Executor wrap(Executor executor) {
    return getResourceUsageExplainer().wrap(executor);
  }

  ResourceUsageFeatureExplainer getResourceUsageExplainer() {
    // NB: inject systemInfo to handle tests where we might mock this out.
    return this.rootQueryInfo.getFeatureExplainer(
        ResourceUsageFeatureExplainer.class,
        () -> new ResourceUsageFeatureExplainer(this.systemInfo));
  }

  public static class IndexPartitionResourceManager implements AutoCloseable {
    private final Scope scope;

    @MustBeClosed
    IndexPartitionResourceManager() {
      this.scope = Scope.noop();
    }

    @MustBeClosed
    IndexPartitionResourceManager(@WillCloseWhenClosed Scope scope) {
      this.scope = scope;
    }

    @Override
    public void close() {
      this.scope.close();
    }
  }

  private static class KeyAndContext {
    private final ContextKey<Explain.QueryInfo> key;
    private final Context context;

    private KeyAndContext(ContextKey<Explain.QueryInfo> key, Context context) {
      this.key = key;
      this.context = context;
    }
  }
}
