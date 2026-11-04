package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Optional;
import java.util.OptionalInt;
import org.bson.BsonDocument;

/**
 * Criteria for approximate (ANN) vector search.
 *
 * <p>{@code numCandidates} may be empty when the user omits numCandidates from the query (auto
 * mode). Use {@link #getNumCandidates()} for the raw user-supplied value (empty in auto mode),
 * {@link #isAutoNumCandidates()} to check the mode, and {@link #resolveNumCandidates(boolean)} for
 * the effective value ({@code clamp(ratio * limit, 100, 10000)} in auto mode, the user value
 * otherwise). The ratio is {@link #AUTO_NUM_CANDIDATES_RATIO}, or {@link
 * #BINARY_AUTO_NUM_CANDIDATES_RATIO} when {@link #isBinaryQuantizedSearch} holds.
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

  /** Lower bound for auto-computed numCandidates. */
  public static final int MIN_NUM_CANDIDATES = 100;

  /** Auto k/limit ratio: auto numCandidates targets 8x the requested limit before clamping. */
  public static final int AUTO_NUM_CANDIDATES_RATIO = 8;

  /**
   * Auto k/limit ratio for binary-quantized fields. Binary quantization loses fidelity in the HNSW
   * graph, so at the default ratio recall falls below float/scalar; doubling the candidate pool
   * restores comparable recall (~0.9 at limit=100). Applied only in auto mode on binary-quantized
   * fields; an explicit user-supplied numCandidates is never changed.
   */
  public static final int BINARY_AUTO_NUM_CANDIDATES_RATIO = 16;

  public ApproximateVectorSearchCriteria {
    if (numCandidates.isPresent()) {
      Check.checkArg(
          limit <= numCandidates.get(), "limit should be less than or equal to numCandidates");
    } else {
      Check.checkArg(limit <= MAX_NUM_CANDIDATES, "limit cannot exceed %s", MAX_NUM_CANDIDATES);
    }
  }

  /** Returns true when numCandidates was omitted (auto mode). */
  public boolean isAutoNumCandidates() {
    return this.numCandidates.isEmpty();
  }

  /** Returns the raw user-supplied numCandidates, or empty in auto mode. */
  public OptionalInt getNumCandidates() {
    return this.numCandidates.map(OptionalInt::of).orElseGet(OptionalInt::empty);
  }

  /**
   * Returns true when a search over a field indexed with {@code fieldQuantization} using {@code
   * queryVector} runs against the field's binary-quantized vectors: the field is binary-quantized
   * and the query vector is float or byte (bit query vectors target the raw bit field). Such
   * searches are rescored and use {@link #BINARY_AUTO_NUM_CANDIDATES_RATIO} in auto mode. This is
   * the single predicate shared by query translation, rescoring selection, and numCandidates
   * metrics so they cannot disagree on the effective value.
   */
  public static boolean isBinaryQuantizedSearch(
      VectorQuantization fieldQuantization, Vector queryVector) {
    if (fieldQuantization != VectorQuantization.BINARY) {
      return false;
    }
    return switch (queryVector.getVectorType()) {
      case FLOAT, BYTE -> true;
      case FLOAT16, BIT -> false;
    };
  }

  /**
   * Returns the effective numCandidates to use for query execution: the user-supplied value, or in
   * auto mode {@code clamp(ratio * limit, MIN_NUM_CANDIDATES, MAX_NUM_CANDIDATES)} where the ratio
   * is {@link #BINARY_AUTO_NUM_CANDIDATES_RATIO} when {@code binaryQuantized} (see {@link
   * #isBinaryQuantizedSearch}) and {@link #AUTO_NUM_CANDIDATES_RATIO} otherwise. An explicit
   * user-supplied numCandidates is always returned unchanged. Never throws.
   *
   * <p>The {@code (long)} multiply avoids int overflow for large limits; {@code Math.clamp(long,
   * int, int)} returns the value clamped into the int bounds. The result is always {@code >=
   * limit}: parsing and the constructor reject {@code limit > MAX_NUM_CANDIDATES} in auto mode.
   */
  public int resolveNumCandidates(boolean binaryQuantized) {
    int ratio = binaryQuantized ? BINARY_AUTO_NUM_CANDIDATES_RATIO : AUTO_NUM_CANDIDATES_RATIO;
    return this.numCandidates.orElseGet(
        () -> Math.clamp((long) ratio * this.limit, MIN_NUM_CANDIDATES, MAX_NUM_CANDIDATES));
  }

  public static ApproximateVectorSearchCriteria fromBson(
      DocumentParser parser, FieldPath path, VectorSearchFilter.Type filterType)
      throws BsonParseException {
    boolean exact = parser.getField(Fields.EXACT).unwrap();
    if (exact) {
      throw new BsonParseException(
          "exact must be false or omitted for ApproximateVectorSearchCriteria", Optional.empty());
    }
    // Empty in auto mode: numCandidates will be computed at query time.
    Optional<Integer> numCandidates = parser.getField(Fields.NUM_CANDIDATES).unwrap();
    int limit = parser.getField(Fields.LIMIT).unwrap();

    if (numCandidates.isPresent() && limit > numCandidates.get()) {
      parser
          .getContext()
          .handleSemanticError("limit should be less than or equal to numCandidates");
    }

    // An explicit numCandidates is already bounded by MAX_NUM_CANDIDATES (and limit <=
    // numCandidates), so only auto mode needs a separate bound on limit.
    if (numCandidates.isEmpty() && limit > MAX_NUM_CANDIDATES) {
      parser
          .getContext()
          .handleSemanticError(String.format("limit cannot exceed %d", MAX_NUM_CANDIDATES));
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
