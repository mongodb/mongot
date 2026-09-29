package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Criteria for approximate (ANN) vector search.
 *
 * <p>{@code numCandidates} may be empty when the {@code enableAutoNumCandidates} feature flag is
 * enabled and the user omits numCandidates from the query. In auto mode, the effective value is
 * computed at query execution time as {@code clamp(8 * limit, 100, 10000)}. Use {@link
 * #getNumCandidates()} to get the user-supplied value (throws if auto) or {@link
 * #isAutoNumCandidates()} to check.
 */
public record ApproximateVectorSearchCriteria(
    FieldPath path,
    Optional<Vector> queryVector,
    Optional<VectorSearchQueryInput> query,
    Optional<VectorSearchFilter> filter,
    Optional<VectorSearchFilter> parentFilter,
    int limit,
    Optional<Integer> numCandidates,
    Optional<ExplainOptions> explainOptions,
    boolean returnStoredSource,
    Optional<VectorEmbeddedOptions> embeddedOptions)
    implements VectorSearchCriteria {

  /** Maximum value for numCandidates, whether user-supplied or auto-computed. */
  public static final int MAX_NUM_CANDIDATES = 10_000;

  public ApproximateVectorSearchCriteria {
    if (numCandidates.isPresent()) {
      Check.checkArg(
          limit <= numCandidates.get(), "limit should be less than or equal to numCandidates");
    }
  }

  /** Returns true when numCandidates was omitted (auto mode). */
  public boolean isAutoNumCandidates() {
    return this.numCandidates.isEmpty();
  }

  /**
   * Returns a new criteria with the numCandidates resolved to the given value. Used by the query
   * execution layer to fill in the auto-computed value before passing to Lucene.
   */
  public ApproximateVectorSearchCriteria withNumCandidates(int resolved) {
    return new ApproximateVectorSearchCriteria(
        this.path,
        this.queryVector,
        this.query,
        this.filter,
        this.parentFilter,
        this.limit,
        Optional.of(resolved),
        this.explainOptions,
        this.returnStoredSource,
        this.embeddedOptions);
  }

  /**
   * Returns the effective numCandidates value. Throws if in auto mode and not yet resolved — call
   * {@link #withNumCandidates} first.
   */
  public int getNumCandidates() {
    return this.numCandidates.orElseThrow(
        () ->
            new IllegalStateException(
                "numCandidates is in auto mode and has not been resolved yet"));
  }

  public static ApproximateVectorSearchCriteria fromBson(
      DocumentParser parser, FieldPath path, VectorSearchFilter.Type filterType)
      throws BsonParseException {
    boolean exact = parser.getField(Fields.EXACT).unwrap();
    if (exact) {
      throw new BsonParseException(
          "exact must be false or omitted for ApproximateVectorSearchCriteria", Optional.empty());
    }
    // Empty in auto mode: numCandidates will be computed at query time. When
    // enableAutoNumCandidates is disabled, the command layer rejects the omission.
    Optional<Integer> numCandidates = parser.getField(Fields.NUM_CANDIDATES).unwrap();
    int limit = parser.getField(Fields.LIMIT).unwrap();

    if (numCandidates.isPresent() && limit > numCandidates.get()) {
      parser
          .getContext()
          .handleSemanticError("limit should be less than or equal to numCandidates");
    }

    var vector = parser.getField(Fields.QUERY_VECTOR).unwrap();
    var queryInput = VectorSearchQueryInput.fromBson(parser);

    VectorSearchCriteria.checkBasicFields(parser, vector, queryInput);

    Optional<VectorSearchFilter> filter = VectorSearchFilter.fromBson(parser, filterType);
    Optional<VectorSearchFilter> parentFilter =
        VectorSearchFilter.fromBsonParentFilter(parser, filterType);

    // Validate that filter and parentFilter are not combined in an OR
    // (they can only be combined with AND)
    VectorSearchCriteria.validateFilterAndParentFilter(parser, filter, parentFilter);

    return new ApproximateVectorSearchCriteria(
        path,
        vector,
        queryInput,
        filter,
        parentFilter,
        limit,
        numCandidates,
        parser.getField(Fields.EXPLAIN_OPTIONS).unwrap(),
        parser.getField(Fields.STORED_SOURCE).unwrap(),
        parser.getField(Fields.NESTED_OPTIONS).unwrap());
  }

  @Override
  public Type getVectorSearchType() {
    return query().map(ignored -> Type.AUTO_EMBEDDING).orElse(Type.APPROXIMATE);
  }

  @Override
  public VectorSearchCriteria withQueryVector(Vector vector) {
    return new ApproximateVectorSearchCriteria(
        this.path,
        Optional.of(vector),
        Optional.empty(),
        this.filter,
        this.parentFilter,
        this.limit,
        this.numCandidates,
        this.explainOptions,
        this.returnStoredSource,
        this.embeddedOptions);
  }

  @Override
  public VectorSearchCriteria withPath(FieldPath path) {
    return new ApproximateVectorSearchCriteria(
        path,
        this.queryVector,
        this.query,
        this.filter,
        this.parentFilter,
        this.limit,
        this.numCandidates,
        this.explainOptions,
        this.returnStoredSource,
        this.embeddedOptions);
  }

  @Override
  public BsonDocument toBson() {
    BsonDocument filterDocument =
        this.filter.map(DocumentEncodable::toBson).orElse(new BsonDocument());
    BsonDocument queryDocument =
        this.query.map(DocumentEncodable::toBson).orElse(new BsonDocument());
    BsonDocumentBuilder builder =
        BsonDocumentBuilder.builder()
            .field(Fields.LIMIT, this.limit())
            .field(Fields.EXACT, false)
            .field(Fields.NUM_CANDIDATES, this.numCandidates)
            .field(Fields.QUERY_VECTOR, this.queryVector())
            .join(queryDocument)
            .field(Fields.EXPLAIN_OPTIONS, this.explainOptions())
            .field(Fields.STORED_SOURCE, this.returnStoredSource())
            .field(Fields.NESTED_OPTIONS, this.embeddedOptions())
            .join(filterDocument);

    if (this.parentFilter.isPresent()) {
      BsonDocument pfDoc = this.parentFilter.get().toBson();
      if (pfDoc.containsKey("filter")) {
        BsonDocument parentFilterDoc = new BsonDocument("parentFilter", pfDoc.get("filter"));
        builder.join(parentFilterDoc);
      }
    }

    return builder.build();
  }
}
