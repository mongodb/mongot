package com.xgen.mongot.index.query;

import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.ExactVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.CompoundClause;
import com.xgen.mongot.index.query.operators.mql.NorClause;
import com.xgen.mongot.index.query.operators.mql.OrClause;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * MaterializedVectorSearchQuery is a type safe wrapper of VectorSearchQuery to be used in
 * VectorIndexReader::query method. AutoEmbedding VectorSearchQuery doesn't have queryVector during
 * creation time, so it needs to be materialized to MaterializedVectorSearchQuery for
 * VectorIndexReader to consume.
 */
public record MaterializedVectorSearchQuery(
    VectorSearchQuery vectorSearchQuery,
    Vector vector,
    Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping,
    Set<FieldPath> embedSourcePaths) {

  public MaterializedVectorSearchQuery(VectorSearchQuery rawVectorSearchQuery, Vector vector) {
    this(rawVectorSearchQuery, vector, Map.of());
  }

  /**
   * Backward-compatible constructor defaulting {@link #embedSourcePaths()} to the queried vector
   * path only. Callers indexing multiple auto-embed sources should use the canonical constructor
   * (passing the full set), else a filter on another source path is wrongly remapped
   * (CLOUDP-402522).
   */
  public MaterializedVectorSearchQuery(
      VectorSearchQuery rawVectorSearchQuery,
      Vector vector,
      Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping) {
    this(
        rawVectorSearchQuery,
        vector,
        autoEmbeddingFieldsMapping,
        Set.of(rawVectorSearchQuery.criteria().path()));
  }

  /**
   * Returns the internal field path by converting user provided query path into internal field path
   * stored in index, return original field path if there is no matching entry in materialized view
   * schema mapping.
   *
   * <p>For example: for user provided query: {'index': 'default', 'path': 'title', 'query': 'xxxx'}
   * and {'autoEmbeddingFieldsMapping': {'title': '_autoEmbed.title'}}, the returned internalPath
   * will be '_autoEmbed.title'.
   */
  public FieldPath internalPath() {
    return this.autoEmbeddingFieldsMapping()
        .getOrDefault(
            this.vectorSearchQuery().criteria().path(), this.vectorSearchQuery().criteria().path());
  }

  public Optional<Vector> queryVector() {
    return Optional.of(vector());
  }

  public Optional<VectorSearchQueryInput> query() {
    return vectorSearchQuery().criteria().query();
  }

  public Optional<VectorSearchFilter> filter() {
    return vectorSearchQuery().criteria().filter();
  }

  public int limit() {
    return vectorSearchQuery().criteria().limit();
  }

  public VectorSearchCriteria.Type getVectorSearchType() {
    return query()
        .map(ignored -> VectorSearchCriteria.Type.AUTO_EMBEDDING)
        .orElse(vectorSearchQuery().criteria().getVectorSearchType());
  }

  public Vector.VectorType queryVectorType() {
    return vector().getVectorType();
  }

  public boolean concurrent() {
    return vectorSearchQuery().concurrent();
  }

  public boolean returnStoredSource() {
    return vectorSearchQuery().returnStoredSource();
  }

  /**
   * Returns a materialized version of the query criteria, ensuring {@link
   * VectorSearchCriteria#queryVector()} is present, and remaps child-level filter clause paths for
   * auto-embed nested indexes to the MV filter field paths (CLOUDP-402522).
   *
   * <p>Remapping uses {@link #autoEmbeddingFieldsMapping()} as the source of truth: a filter clause
   * with a direct mapping entry is rewritten to the MV path, unless its path is an auto-embed
   * source (see {@link #embedSourcePaths()}) whose entry points at a vector field — then it's left
   * as is.
   */
  public VectorSearchCriteria materializedCriteria() {
    VectorSearchCriteria criteria = vectorSearchQuery().criteria();
    Optional<VectorSearchFilter> remappedFilter = remapFilterPaths(criteria.filter());
    return switch (criteria) {
      case ApproximateVectorSearchCriteria approximate ->
          new ApproximateVectorSearchCriteria(
              internalPath(),
              Optional.of(this.vector),
              // Exactly one of queryVector or query can be present
              Optional.empty(),
              remappedFilter,
              approximate.parentFilter(),
              approximate.limit(),
              approximate.numCandidates(),
              approximate.explainOptions(),
              approximate.returnStoredSource(),
              approximate.embeddedOptions());
      case ExactVectorSearchCriteria exact ->
          new ExactVectorSearchCriteria(
              internalPath(),
              Optional.of(this.vector),
              // Exactly one of queryVector or query can be present
              Optional.empty(),
              remappedFilter,
              exact.parentFilter(),
              exact.limit(),
              exact.returnStoredSource(),
              exact.embeddedOptions());
    };
  }

  private Optional<VectorSearchFilter> remapFilterPaths(Optional<VectorSearchFilter> filter) {
    if (filter.isEmpty() || autoEmbeddingFieldsMapping().isEmpty()) {
      return filter;
    }
    return filter.map(
        f ->
            switch (f) {
              case VectorSearchFilter.ClauseFilter clauseFilter ->
                  new VectorSearchFilter.ClauseFilter(remapClausePaths(clauseFilter.clause()));
              // OperatorFilter is used in $search.vectorSearch where the auto-embed nested filter
              // remapping is not applicable (no FILTER-under-nestedRoot remap entries are
              // produced for that path). Leave it untouched.
              case VectorSearchFilter.OperatorFilter operatorFilter -> operatorFilter;
            });
  }

  private Clause remapClausePaths(Clause clause) {
    return switch (clause) {
      case SimpleClause simple -> {
        // The schema mapping carries entries for both AUTO_EMBED fields (vector path remap) and
        // FILTER-under-nestedRoot fields (filter path remap). For filter clauses we must skip
        // every AUTO_EMBED-source entry, not just the queried one: a filter clause whose path is
        // an auto-embed source path is, by construction, targeting a vector field, and rewriting
        // it via the AUTO_EMBED entry would route the filter into that field's MV vector path. An
        // index may declare multiple auto-embed fields, so comparing against the single queried
        // vector path is insufficient; we check membership in the full set of auto-embed source
        // paths. The MV may also declare an explicit FILTER field at the same source path (top-
        // level auto-embed indexes); in that case the FILTER field stays at the source path and
        // the filter clause must too. See LuceneVectorQueryFactoryDistributorTest /
        // testApproximateVectorQueryWithAutoEmbeddingFieldsMappingAndFilter and CLOUDP-402522.
        if (embedSourcePaths().contains(simple.path())) {
          yield simple;
        }
        FieldPath remapped =
            autoEmbeddingFieldsMapping().getOrDefault(simple.path(), simple.path());
        yield remapped.equals(simple.path())
            ? simple
            : new SimpleClause(remapped, simple.mqlFilterOperators());
      }
      case CompoundClause compound -> {
        List<Clause> remappedChildren =
            compound.getClauses().stream().map(this::remapClausePaths).toList();
        yield switch (compound) {
          // Recreate with explicit-and constructor to preserve $and serialization shape, since
          // createImplicitAndClause(...) collapses single-element lists into the bare child and
          // would change AndClause-specific output for a top-level $and: [ ... single ... ].
          case AndClause and ->
              new AndClause(remappedChildren, and.toBson().asDocument().containsKey("$and"));
          case OrClause ignored -> new OrClause(remappedChildren);
          case NorClause ignored -> new NorClause(remappedChildren);
        };
      }
    };
  }
}
