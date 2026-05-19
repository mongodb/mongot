package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Defines the core configuration for executing a vector search query.
 *
 * <p>A {@code VectorSearchCriteria} encapsulates the search parameters (e.g., field path, vector
 * input, filter, and limit) along with the type of search to perform (approximate or exact).
 */
public sealed interface VectorSearchCriteria extends DocumentEncodable
    permits ApproximateVectorSearchCriteria, ExactVectorSearchCriteria {

  FieldPath path();

  Optional<Vector> queryVector();

  Optional<VectorSearchQueryInput> query();

  Optional<VectorSearchFilter> filter();

  Optional<VectorSearchFilter> parentFilter();

  int limit();

  boolean returnStoredSource();

  Optional<VectorEmbeddedOptions> embeddedOptions();

  Type getVectorSearchType();

  enum Type {
    EXACT,
    APPROXIMATE,
    AUTO_EMBEDDING
  }

  record ExplainOptions(List<BsonValue> traceDocuments) implements DocumentEncodable {
    private static class Fields {

      public static final Field.WithDefault<List<BsonValue>> TRACE_DOCUMENT_IDS =
          Field.builder("traceDocumentIds")
              .listOf(Value.builder().unparsedValueField().required())
              .mustNotBeEmpty()
              .optional()
              .withDefault(List.of());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .fieldOmitDefaultValue(Fields.TRACE_DOCUMENT_IDS, this.traceDocuments)
          .build();
    }

    public static ExplainOptions fromBson(DocumentParser parser) throws BsonParseException {
      return new ExplainOptions(parser.getField(ExplainOptions.Fields.TRACE_DOCUMENT_IDS).unwrap());
    }
  }

  class Fields {

    static final Field.Required<Integer> LIMIT =
        Field.builder("limit").intField().mustBePositive().required();

    static final Field.WithDefault<Boolean> EXACT =
        Field.builder("exact").booleanField().optional().withDefault(false);

    static final Field.Optional<Vector> QUERY_VECTOR =
        Field.builder("queryVector").classField(Vector::fromBson).optional().noDefault();

    // Required in Approximate, omitted in Exact. We do those validations in their respective
    // classes and set this field to Optional.
    static final Field.Optional<Integer> NUM_CANDIDATES =
        Field.builder("numCandidates")
            .intField()
            .mustBeWithinBounds(Range.of(1, 10_000))
            .optional()
            .noDefault();

    public static final Field.Optional<ExplainOptions> EXPLAIN_OPTIONS =
        Field.builder("explainOptions")
            .classField(ExplainOptions::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.WithDefault<Boolean> STORED_SOURCE =
        Field.builder("returnStoredSource").booleanField().optional().withDefault(false);

    public static final Field.Optional<VectorEmbeddedOptions> NESTED_OPTIONS =
        Field.builder("nestedOptions")
            .classField(VectorEmbeddedOptions::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  static VectorSearchCriteria fromBson(
      DocumentParser parser, FieldPath path, VectorSearchFilter.Type filterType)
      throws BsonParseException {
    if (parser.getField(Fields.EXACT).unwrap()) {
      return ExactVectorSearchCriteria.fromBson(parser, path, filterType);
    }
    return ApproximateVectorSearchCriteria.fromBson(parser, path, filterType);
  }

  /**
   * Validates query vector and auto-embedding input fields shared by exact and approximate vector
   * search.
   */
  static void checkBasicFields(
      DocumentParser parser,
      Optional<Vector> queryVector,
      Optional<VectorSearchQueryInput> queryInput)
      throws BsonParseException {

    // Check that there is one and only one of query and queryVector.
    if (queryVector.isPresent() == queryInput.isPresent()) {
      parser
          .getContext()
          .handleSemanticError("Exactly one and only one of query and queryVector can be present");
    }
    if (queryVector.isEmpty()) {
      // Skip checks for auto-embedding.
      return;
    }
    if (queryVector.get() instanceof FloatVector floatVector) {
      var v = floatVector.getFloatVector();
      for (int i = 0; i < v.length; i++) {
        if (!Float.isFinite(v[i])) {
          parser
              .getContext()
              .handleSemanticError(
                  String.format(
                      "'queryVector' contains a non-finite value (Infinity or NaN) at position"
                          + " [%d]. All values in \"queryVector\" must be finite.",
                      i));
        }
      }
    }
  }

  /**
   * Validates that filter and parentFilter cannot be combined in an OR operation.
   *
   * <p>When both filter and parentFilter are present, they are implicitly combined with AND. This
   * validation ensures users understand this constraint. Note: Each filter can independently
   * contain OR clauses within themselves.
   *
   * @param parser the document parser for error reporting
   * @param filter the optional filter
   * @param parentFilter the optional parent filter
   * @throws BsonParseException if validation fails
   */
  static void validateFilterAndParentFilter(
      DocumentParser parser,
      Optional<VectorSearchFilter> filter,
      Optional<VectorSearchFilter> parentFilter)
      throws BsonParseException {
    // The validation is implicit: if both are present, they are combined with AND.
    // There's no way to specify OR between them in the current API design.
    // This method is a placeholder for future validation if needed.
    // For now, we just document the behavior.
  }
}
