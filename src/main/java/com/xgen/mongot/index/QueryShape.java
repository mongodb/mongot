package com.xgen.mongot.index;

import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.operators.CompoundClause;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Renders the structure of a parsed query for the service log: the operator types it is built from,
 * which clause each sits in, and how they nest — but none of the values the user supplied.
 *
 * <p>Example output: {@code compound[must:text, filter:equals|range]}
 *
 * <p>Every token this produces comes from a compile-time enum — {@link Operator.Type}, {@link
 * Collector.Type}, {@link VectorSearchCriteria.Type}, {@link MqlFilterOperator.Category} — or a
 * fixed clause name. That is the property which makes the result safe to write to an
 * operator-readable log, and it is the one to preserve when extending this class: never emit a
 * field path, a search term, a filter value, a vector or any other value read off the query. Those
 * are customer content, and the service log is shipped to log aggregation.
 *
 * <p>Output is bounded in both depth and breadth. The user controls how deeply and widely a query
 * nests, so without those bounds the caller would also be handing the user control of how much log
 * volume a single failing query can produce.
 */
public final class QueryShape {

  /** Nesting depth past which sub-operators collapse to {@code [...]}. */
  private static final int MAX_DEPTH = 4;

  /** Number of operators rendered per compound clause before the rest are summarised. */
  private static final int MAX_OPERATORS_PER_CLAUSE = 8;

  /** Returned when a query cannot be described, so shape rendering never masks the real error. */
  private static final String UNKNOWN = "unknown";

  private QueryShape() {}

  /**
   * Describes the operator structure of {@code query}, without any user-supplied values.
   *
   * <p>Never throws: this is called from exception handlers that are already reporting a failure,
   * and a problem rendering the shape must not displace the failure being reported.
   */
  public static String of(Query query) {
    try {
      return switch (query) {
        case SearchQuery searchQuery -> describeSearchQuery(searchQuery);
        case VectorSearchQuery vectorQuery -> describeVectorQuery(vectorQuery);
      };
    } catch (RuntimeException e) {
      return UNKNOWN;
    }
  }

  private static String describeSearchQuery(SearchQuery query) {
    return switch (query) {
      case OperatorQuery operatorQuery -> describeOperator(operatorQuery.operator(), 0);
      case CollectorQuery collectorQuery -> {
        String collector = lowerCase(collectorQuery.collector().getType().name());
        yield collectorQuery
            .collector()
            .getOperator()
            .map(operator -> collector + "[" + describeOperator(operator, 1) + "]")
            .orElse(collector);
      }
    };
  }

  private static String describeVectorQuery(VectorSearchQuery query) {
    VectorSearchCriteria criteria = query.criteria();
    StringJoiner parts =
        new StringJoiner(", ", "vectorSearch[", "]")
            .add(lowerCase(criteria.getVectorSearchType().name()));
    criteria.filter().ifPresent(filter -> parts.add("filter:" + describeFilter(filter)));
    criteria
        .parentFilter()
        .ifPresent(filter -> parts.add("parentFilter:" + describeFilter(filter)));
    return parts.toString();
  }

  private static String describeFilter(VectorSearchFilter filter) {
    return switch (filter) {
      case VectorSearchFilter.OperatorFilter operatorFilter ->
          describeOperator(operatorFilter.operator(), 1);
      // Reuses the clause walk QueryMetricsRecorder already performs to pull filter categories out
      // of an MQL clause; like that walk, it yields enum categories only.
      case VectorSearchFilter.ClauseFilter clauseFilter ->
          QueryMetricsRecorder.getAllLeafOperators(clauseFilter.clause())
              .map(category -> lowerCase(category.name()))
              .distinct()
              .limit(MAX_OPERATORS_PER_CLAUSE)
              .collect(Collectors.joining("|"));
    };
  }

  private static String describeOperator(Operator operator, int depth) {
    String type = operator.getType().getName();
    if (!(operator instanceof CompoundOperator compound)) {
      return type;
    }
    if (depth >= MAX_DEPTH) {
      return type + "[...]";
    }

    StringJoiner clauses = new StringJoiner(", ", type + "[", "]");
    clauses.setEmptyValue(type);
    appendClause(clauses, "must", compound.must(), depth);
    appendClause(clauses, "mustNot", compound.mustNot(), depth);
    appendClause(clauses, "should", compound.should(), depth);
    appendClause(clauses, "filter", compound.filter(), depth);
    return clauses.toString();
  }

  private static void appendClause(
      StringJoiner clauses, String name, CompoundClause clause, int depth) {
    if (clause.isEmpty()) {
      return;
    }

    List<? extends Operator> operators = clause.operators();
    StringJoiner rendered = new StringJoiner("|");
    operators.stream()
        .limit(MAX_OPERATORS_PER_CLAUSE)
        .forEach(operator -> rendered.add(describeOperator(operator, depth + 1)));
    if (operators.size() > MAX_OPERATORS_PER_CLAUSE) {
      rendered.add("+" + (operators.size() - MAX_OPERATORS_PER_CLAUSE) + " more");
    }

    clauses.add(name + ":" + rendered);
  }

  private static String lowerCase(String enumName) {
    return enumName.toLowerCase(Locale.ROOT);
  }
}
