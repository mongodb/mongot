package com.xgen.mongot.server.command.search;

import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.CompoundClause;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Rejects vector queries that reach auto-embed fields of a search index without the $search
 * auto-embed rewrite: $vectorSearch stages, which cannot use auto-embed fields at all, and $search
 * vectorSearch paths under the internal materialized-view namespace.
 */
final class VectorSearchStageAutoEmbedValidator {

  /** Reserved for materialized-view fields; index definitions cannot map paths under it. */
  private static final FieldPath RESERVED_AUTO_EMBED_ROOT = FieldPath.newRoot("_autoEmbed");

  private VectorSearchStageAutoEmbedValidator() {}

  /**
   * Rejects the internal materialized-view namespace. A user query must name the auto-embed source
   * path so the rewrite can map it to the materialized-view vector path and reconcile the
   * queryVector dtype.
   */
  static void validateNotReservedAutoEmbedPath(FieldPath path) throws InvalidQueryException {
    if (path.equals(RESERVED_AUTO_EMBED_ROOT) || path.isChildOf(RESERVED_AUTO_EMBED_ROOT)) {
      throw new InvalidQueryException(
          String.format(
              "Path '%s' is reserved for internal automated embeddings fields. Query the"
                  + " field by its source path instead.",
              path));
    }
  }

  /**
   * Validates that a $vectorSearch stage against a search index does not touch auto-embed fields.
   * The stage cannot embed query text or resolve auto-embed paths, so it may only query other
   * vector fields, such as knnVector, and filter on paths the served index can answer.
   *
   * @param autoEmbedSourcePaths user-facing paths declared as autoEmbed, each mapped to the
   *     materialized-view path of its vector
   * @param servedDefinition the definition of the index generation executing the query
   * @throws InvalidQueryException if the stage names a path under the reserved {@code _autoEmbed}
   *     namespace, queries an auto-embed source or vector path, or filters on a source path the
   *     served index embeds and does not map with another type
   */
  static void validateVectorSearchStage(
      VectorSearchQuery query,
      Map<FieldPath, FieldPath> autoEmbedSourcePaths,
      SearchIndexDefinition servedDefinition)
      throws InvalidQueryException {
    FieldPath path = query.criteria().path();
    List<FieldPath> filterPaths = new ArrayList<>();
    collectClauseFilterPaths(query.criteria().filter(), filterPaths);
    collectClauseFilterPaths(query.criteria().parentFilter(), filterPaths);
    // The internal namespace is invalid on the stage path and on its filters whatever the mapping
    // resolves to. Report it before the autoEmbed type errors, with the same message
    // $search.vectorSearch returns for the same input.
    validateNotReservedAutoEmbedPath(path);
    for (FieldPath filterPath : filterPaths) {
      validateNotReservedAutoEmbedPath(filterPath);
    }
    Collection<FieldPath> vectorPaths = autoEmbedSourcePaths.values();
    // The catalog definition can lag or lead the generation serving the query, so a source path is
    // auto-embed for this query only when the served definition actually embeds it.
    boolean pathEmbedsServed =
        autoEmbedSourcePaths.entrySet().stream()
            .anyMatch(
                entry ->
                    entry.getKey().equals(path)
                        && isEmbedded(servedDefinition, entry.getKey(), entry.getValue()));
    // Without a materialized-view schema a vector path falls back to its source path. Such a path
    // is decided by the served definition: pathEmbedsServed for the stage path and the embed-only
    // check below for filters.
    Predicate<FieldPath> isVectorOnlyPath =
        p -> vectorPaths.contains(p) && !autoEmbedSourcePaths.containsKey(p);
    if (pathEmbedsServed
        || isVectorOnlyPath.test(path)
        || filterPaths.stream().anyMatch(isVectorOnlyPath)) {
      throw new InvalidQueryException(
          String.format(
              "$vectorSearch cannot query type:autoEmbed fields in search index '%s'. Use the"
                  + " vectorSearch operator in $search instead.",
              query.index()));
    }
    for (FieldPath filterPath : filterPaths) {
      FieldPath materializedViewPath = autoEmbedSourcePaths.get(filterPath);
      if (materializedViewPath != null
          && isEmbedded(servedDefinition, filterPath, materializedViewPath)
          && !isServedWithOtherType(servedDefinition, filterPath)) {
        throw new InvalidQueryException(
            String.format(
                "Path '%s' is a type:autoEmbed field and can only be queried with the"
                    + " vectorSearch operator. To use it in other operators, filters, sort,"
                    + " highlight, or facets, also index it with another type, such as string.",
                filterPath));
      }
    }
  }

  /**
   * Returns whether the served index embeds {@code path}. The catalog can declare an auto-embed
   * field that the served generation does not have yet while a definition change is being built.
   */
  private static boolean isEmbedded(
      SearchIndexDefinition servedDefinition, FieldPath path, FieldPath materializedViewPath) {
    return servedDefinition
            .getStaticFieldDefinition(materializedViewPath)
            .flatMap(FieldDefinition::searchIndexVectorFieldDefinition)
            .isPresent()
        || servedDefinition
            .getStaticFieldDefinition(path)
            .flatMap(FieldDefinition::searchAutoEmbedFieldDefinition)
            .isPresent();
  }

  /**
   * Returns whether {@code path} is also served with a type that can answer a filter. Vector types
   * do not count: without a materialized-view schema the generated vector sits at the source path.
   */
  private static boolean isServedWithOtherType(
      SearchIndexDefinition servedDefinition, FieldPath path) {
    return servedDefinition
        .getStaticFieldDefinition(path)
        .map(
            definition ->
                definition.hasFieldDefinitionsOtherThan(
                    FieldTypeDefinition.Type.AUTO_EMBED,
                    FieldTypeDefinition.Type.VECTOR,
                    FieldTypeDefinition.Type.KNN_VECTOR))
        .orElse(false);
  }

  private static void collectClauseFilterPaths(
      Optional<VectorSearchFilter> filter, List<FieldPath> paths) {
    if (filter.isPresent()
        && filter.get() instanceof VectorSearchFilter.ClauseFilter clauseFilter) {
      collectClausePaths(clauseFilter.clause(), paths);
    }
  }

  private static void collectClausePaths(Clause clause, List<FieldPath> paths) {
    switch (clause) {
      case SimpleClause simpleClause -> paths.add(simpleClause.path());
      case CompoundClause compoundClause ->
          compoundClause.getClauses().forEach(child -> collectClausePaths(child, paths));
    }
  }
}
