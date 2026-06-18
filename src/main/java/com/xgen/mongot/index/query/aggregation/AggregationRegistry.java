package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Registry for aggregation type name to their spec factories.
 *
 * <p>Populated at startup and immutable after that. Keyed by name only (no field type). The same
 * spec class handles all field types for a given operator; field-type-specific dispatch can be
 * added later if needed.
 *
 * <p>This framework-only registry starts with an empty default instance -- no production specs are
 * wired up here. Concrete aggregation specs (avg, sum, terms, cardinality, ...) are registered by
 * follow-up PRs once the query syntax is finalized.
 *
 * <p>Startup code (and tests) use {@link #install(Map, Map)} to install the global singleton.
 */
public class AggregationRegistry {

  /**
   * Factory for bucket (group) aggregation specs. Receives the user-assigned aggregation name, the
   * resolved field path, and the list of already-parsed child specs.
   */
  @FunctionalInterface
  public interface GroupSpecSupplier {
    GroupAggregationSpec build(String name, FieldPath path, List<AggregationSpec> children);
  }

  /**
   * Factory for leaf (operator) aggregation specs. Receives the user-assigned aggregation name and
   * the resolved field path.
   */
  @FunctionalInterface
  public interface OperatorSupplier {
    OperatorAggregationSpec build(String name, FieldPath path);
  }

  /**
   * Global singleton, installed via {@link #install(Map, Map)} during mongot startup exactly once.
   * Volatile so a registry installed by the startup thread is safely visible to query threads.
   */
  private static volatile AggregationRegistry instance =
      new AggregationRegistry(Map.of(), Map.of());

  private final Map<String, GroupSpecSupplier> groupRegistry;
  private final Map<String, OperatorSupplier> operatorRegistry;

  private AggregationRegistry(
      Map<String, GroupSpecSupplier> groupRegistry,
      Map<String, OperatorSupplier> operatorRegistry) {
    this.groupRegistry = Map.copyOf(groupRegistry);
    this.operatorRegistry = Map.copyOf(operatorRegistry);
  }

  /** Return the installed global registry. */
  public static AggregationRegistry getInstance() {
    return instance;
  }

  /**
   * Build a registry from the given factories and install it as the global singleton. Expected to
   * be called exactly once during mongot startup, before any query references the registry.
   */
  static void install(
      Map<String, GroupSpecSupplier> groupMap, Map<String, OperatorSupplier> operatorMap) {
    instance = new AggregationRegistry(groupMap, operatorMap);
  }

  public GroupSpecSupplier getGroupSpecSupplier(String name) {
    Optional<GroupSpecSupplier> supplier = Optional.ofNullable(this.groupRegistry.get(name));
    return supplier.orElseThrow(
        () -> new IllegalArgumentException("No group spec supplier for: " + name));
  }

  public OperatorSupplier getOperatorSupplier(String name) {
    Optional<OperatorSupplier> supplier = Optional.ofNullable(this.operatorRegistry.get(name));
    return supplier.orElseThrow(
        () -> new IllegalArgumentException("No operator spec supplier for: " + name));
  }
}
