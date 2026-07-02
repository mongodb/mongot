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

public record ApproximateVectorSearchCriteria(
    FieldPath path,
    Optional<Vector> queryVector,
    Optional<VectorSearchQueryInput> query,
    Optional<VectorSearchFilter> filter,
    Optional<VectorSearchFilter> parentFilter,
    int limit,
    int numCandidates,
    Optional<ExplainOptions> explainOptions,
    boolean returnStoredSource,
    Optional<VectorEmbeddedOptions> embeddedOptions)
    implements VectorSearchCriteria {

  public ApproximateVectorSearchCriteria {
    Check.checkArg(limit <= numCandidates, "limit should be less than or equal to numCandidates");
  }

  public static ApproximateVectorSearchCriteria fromBson(
      DocumentParser parser, FieldPath path, VectorSearchFilter.Type filterType)
      throws BsonParseException {
    boolean exact = parser.getField(Fields.EXACT).unwrap();
    if (exact) {
      throw new BsonParseException(
          "exact must be false or omitted for ApproximateVectorSearchCriteria", Optional.empty());
    }
    Optional<Integer> wrappedNumCandidates = parser.getField(Fields.NUM_CANDIDATES).unwrap();
    if (wrappedNumCandidates.isEmpty()) {
      parser
          .getContext()
          .handleSemanticError("numCandidates is required for approximate vector search");
    }
    int numCandidates = wrappedNumCandidates.get();
    int limit = parser.getField(Fields.LIMIT).unwrap();
    if (limit > numCandidates) {
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
            .field(Fields.NUM_CANDIDATES, Optional.of(this.numCandidates))
            .field(Fields.QUERY_VECTOR, this.queryVector())
            .join(queryDocument)
            .field(Fields.EXPLAIN_OPTIONS, this.explainOptions())
            .field(Fields.STORED_SOURCE, this.returnStoredSource())
            .field(Fields.NESTED_OPTIONS, this.embeddedOptions())
            .join(filterDocument);

    // Add parentFilter with the correct key name
    if (this.parentFilter.isPresent()) {
      BsonDocument pfDoc = this.parentFilter.get().toBson();
      // Extract the filter content and add it under "parentFilter" key
      if (pfDoc.containsKey("filter")) {
        BsonDocument parentFilterDoc = new BsonDocument("parentFilter", pfDoc.get("filter"));
        builder.join(parentFilterDoc);
      }
    }

    return builder.build();
  }
}
