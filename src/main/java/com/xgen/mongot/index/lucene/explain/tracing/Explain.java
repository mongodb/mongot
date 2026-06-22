package com.xgen.mongot.index.lucene.explain.tracing;

import static com.xgen.mongot.index.definition.IndexDefinition.Fields.NUM_PARTITIONS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.MutableTypeToInstanceMap;
import com.google.errorprone.annotations.MustBeClosed;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.util.mongodb.MongoDbVersion;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An API for logging information as part of an <code>explain()</code> query.
 *
 * <p>This class is not instantiated directly and call be called from any code path where you wish
 * to add information to the explain response. These calls will log for explain queries and no-op
 * for all others. To ensure that your information is actually logged you will need to enable this
 * within a scope:
 *
 * <pre>
 *   try (var unused = Explain.setup(Verbosity.QUERY_PLANNER) {
 *     Explain.logDoc("someStuff", getSomeStuff());
 *     makeACallThatAlsoExplainsSomeStuff();
 *     // We won't do the expensive thing in this case because verbosity is at a lower level.
 *     if (Explain.atVerbosity(Verbosity.EXECUTION_STATS) {
 *       logSomethingVeryExpensive();
 *     }
 *     return Explain.collect();
 *   }
 * </pre>
 *
 * <p><code>Explain</code> state is curried through OTEL {@link Context} so if the query is executed
 * across multiple threads you will want to <code>wrap(Executor)</code> or similar.
 */
public class Explain {
  public static final MongoDbVersion FIRST_VERSION_KILLS_CURSORS_EXPLAIN =
      new MongoDbVersion(8, 1, 0);
  private static final Logger LOG = LoggerFactory.getLogger(Explain.class);

  // Do not instantiate
  private Explain() {}

  // NB: the ordinal values of these is meaningful to ExplainQueryState, which uses them to decide
  // if the verbosity exceeds a certain threshold.
  public enum Verbosity {
    QUERY_PLANNER("queryPlanner"),
    EXECUTION_STATS("executionStats"),
    ALL_PLANS_EXECUTION("allPlansExecution");

    private final String name;

    Verbosity(String name) {
      this.name = name;
    }

    public boolean isGreaterThan(Verbosity other) {
      return this.ordinal() > other.ordinal();
    }

    public boolean isLessThanOrEqual(Verbosity other) {
      return this.ordinal() <= other.ordinal();
    }

    public String getName() {
      return this.name;
    }
  }

  /** Container for information about the current explain query. */
  public static class QueryInfo {
    private final Verbosity verbosity;
    private final MutableTypeToInstanceMap<FeatureExplainer> featureExplainers;

    public QueryInfo(Verbosity verbosity) {
      this.verbosity = verbosity;
      this.featureExplainers = new MutableTypeToInstanceMap<>();
    }

    public Verbosity getVerbosity() {
      return this.verbosity;
    }

    /**
     * Get a typed FeatureExplainer for this query or create it if needed.
     *
     * <p>All explainers accessed through this API will be used to add data to the explain message.
     * This method is safe for concurrent access.
     */
    // NB: we must pass the Class along with a Supplier to this method because of the way the type
    // is erased during compilation, see:
    // http://www.angelikalanger.com/GenericsFAQ/FAQSections/ParameterizedTypes.html#FAQ106
    public <T extends FeatureExplainer> T getFeatureExplainer(Class<T> clazz, Supplier<T> factory) {
      synchronized (this.featureExplainers) {
        return Optional.ofNullable(this.featureExplainers.getInstance(clazz))
            .orElseGet(
                () -> {
                  var featureExplainer = factory.get();
                  this.featureExplainers.putInstance(clazz, featureExplainer);
                  return featureExplainer;
                });
      }
    }

    /**
     * Get a typed FeatureExplainer for this query if it exists.
     *
     * <p>All explainers accessed through this API will be used to add data to the explain message.
     * This method is safe for concurrent access.
     */
    public <T extends FeatureExplainer> Optional<T> getFeatureExplainer(Class<T> clazz) {
      synchronized (this.featureExplainers) {
        return Optional.ofNullable(this.featureExplainers.getInstance(clazz));
      }
    }

    synchronized ImmutableList<FeatureExplainer> getFeatureExplainers() {
      return ImmutableList.copyOf(this.featureExplainers.values());
    }
  }

  /**
   * Setup to explain the current query at <code>verbosity</code>.
   *
   * @param verbosity at which we should record
   * @param numPartitions the number of indexPartitions to create Explain sub-contexts for
   * @return a <code>Scope</code> context for explain functionality. Outside of this scope no
   *     explain information will be recorded.
   */
  @MustBeClosed
  @VisibleForTesting
  static Scope setup(Verbosity verbosity, int numPartitions) {
    if (isEnabled()) {
      // Making setup() completely re-entrant may yield partial explain results at each collect()
      // call, which would be confusing. Allowing re-entry may discard data if settings change
      // (different verbosities, etc). Unfortunately we cannot disallow this entirely as this is
      // the mechanism by which FakeExplain injects a value to collect(), and there aren't any good
      // ways to avoid FakeExplain that work on rhel76/rhel80.
      LOG.warn(
          "Explain.setup() is not re-entrant; ignoring setup call. "
              + "This may happen in tests using FakeExplain.");
      return Scope.noop();
    }

    return Context.current()
        .with(new ExplainQueryState(new QueryInfo(verbosity), numPartitions))
        .makeCurrent();
  }

  /**
   * Setup to explain the current query when <code>explainDefinition.isPresent()</code>.
   *
   * @param verbosity indicates if the query should be explained and if so at what level.
   * @param numPartitions the number of indexPartitions to create Explain sub-contexts for
   * @return a <code>Scope</code> context for explain functionality. Outside of this scope no
   *     explain information will be recorded.
   */
  @MustBeClosed
  public static Scope setup(Optional<Verbosity> verbosity, Optional<Integer> numPartitions) {
    if (verbosity.isPresent()) {
      return setup(verbosity.get(), numPartitions.orElse(NUM_PARTITIONS.getDefaultValue()));
    } else {
      return Scope.noop();
    }
  }

  /**
   * Setup to explain the current query when a previous <code>explainQueryState.isPresent()</code>
   */
  @MustBeClosed
  public static Scope setup(Optional<ExplainQueryState> maybeExplainState) {
    if (maybeExplainState.isPresent()) {
      ExplainQueryState explainQueryState = maybeExplainState.get();
      return setup(explainQueryState);
    } else {
      return Scope.noop();
    }
  }

  @VisibleForTesting
  @MustBeClosed
  static Scope setup(ExplainQueryState explainQueryState) {
    explainQueryState.getResourceUsageExplainer().refreshCurrentThreadAndUpdateState();
    return Context.current().with(explainQueryState).makeCurrent();
  }

  @MustBeClosed
  public static ExplainQueryState.IndexPartitionResourceManager
      maybeEnterIndexPartitionQueryContext(int indexPartitionId) {
    if (!isEnabled()) {
      return new ExplainQueryState.IndexPartitionResourceManager();
    } else if (ExplainQueryState.getFromContext().get().getNumPartitions()
        == NUM_PARTITIONS.getDefaultValue()) {
      LOG.warn("Trying to enter index partition query context when none are present");
      return new ExplainQueryState.IndexPartitionResourceManager();
    }

    ExplainQueryState queryState = ExplainQueryState.getFromContext().get();
    Scope scope = queryState.enterIndexPartitionQueryContext(indexPartitionId);
    return new ExplainQueryState.IndexPartitionResourceManager(scope);
  }

  /**
   * Wrap executor to record explain information in all closures executed this way. This returns the
   * input executor if the request is not being explained.
   *
   * @param executor to record explain information on.
   * @return an <code>Executor</code> that records explain information for passed closures.
   */
  public static Executor maybeWrap(Executor executor) {
    return ExplainQueryState.getFromContext().map(state -> state.wrap(executor)).orElse(executor);
  }

  /**
   * Returns QueryInfo associated with the current explain query if present.
   *
   * @return the QueryInfo
   */
  public static Optional<QueryInfo> getQueryInfo() {
    return ExplainQueryState.getFromContext().map(ExplainQueryState::getQueryInfo);
  }

  /** Returns the ExplainQueryState associated with the current explain query if present. */
  public static Optional<ExplainQueryState> getExplainQueryState() {
    return ExplainQueryState.getFromContext();
  }

  /**
   * Check if explain is enabled for this query.
   *
   * @return true if explain is enabled for this query.
   */
  public static boolean isEnabled() {
    return ExplainQueryState.getFromContext().isPresent();
  }

  /**
   * Check if the current request is being explained at a level greater than or equal to verbosity.
   * Use this to avoid expensive computations in queries that are not being explained.
   *
   * @param verbosity minimum verbosity at which true should be returned.
   * @return true if the query is being explained at least <code>verbosity</code>.
   */
  public static boolean atVerbosity(Verbosity verbosity) {
    return ExplainQueryState.getFromContext()
        .filter(state -> state.getQueryInfo().getVerbosity().ordinal() >= verbosity.ordinal())
        .isPresent();
  }

  /**
   * Collects all explain data from the current query if explain is on and does not exceed
   * BsonDocument size limit.
   *
   * @return <code>ExplainInformation</code>.
   */
  public static Optional<SearchExplainInformation> collect() throws ExplainTooLargeException {
    Optional<SearchExplainInformation> explainInformation =
        ExplainQueryState.getFromContext().map(ExplainQueryState::collect);

    if (explainInformation.isPresent()) {
      ExplainTooLargeException.validate(explainInformation.get());
    }

    return explainInformation;
  }
}
