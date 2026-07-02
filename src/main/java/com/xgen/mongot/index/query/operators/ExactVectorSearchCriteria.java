package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Optional;
import org.bson.BsonDocument;

public record ExactVectorSearchCriteria(
    FieldPath path,
    Optional<Vector> queryVector,
    Optional<VectorSearchQueryInput> query,
    Optional<VectorSearchFilter> filter,
    Optional<VectorSearchFilter> parentFilter,
    int limit,
    boolean returnStoredSource,
    Optional<VectorEmbeddedOptions> embeddedOptions)
    implements VectorSearchCriteria {

  public static ExactVectorSearchCriteria fromBson(
      DocumentParser parser, FieldPath path, VectorSearchFilter.Type filterType)
      throws BsonParseException {
    if (!parser.getField(Fields.EXACT).unwrap()) {
      parser.getContext().handleSemanticError("exact must be true for ExactVectorSearchQuery");
    }

    if (parser.getField(Fields.NUM_CANDIDATES).unwrap().isPresent()) {
      parser
          .getContext()
          .handleSemanticError("\"numCandidates\" must be omitted when \"exact\" is set to true");
    }

    Optional<Vector> vector = parser.getField(Fields.QUERY_VECTOR).unwrap();
    Optional<VectorSearchQueryInput> queryInput = VectorSearchQueryInput.fromBson(parser);

    VectorSearchCriteria.checkBasicFields(parser, vector, queryInput);

    Optional<VectorSearchFilter> filter = VectorSearchFilter.fromBson(parser, filterType);
    Optional<VectorSearchFilter> parentFilter =
        VectorSearchFilter.fromBsonParentFilter(parser, filterType);

    // Validate that filter and parentFilter are not combined in an OR
    // (they can only be combined with AND)
    VectorSearchCriteria.validateFilterAndParentFilter(parser, filter, parentFilter);

    return new ExactVectorSearchCriteria(
        path,
        vector,
        queryInput,
        filter,
        parentFilter,
        parser.getField(Fields.LIMIT).unwrap(),
        parser.getField(Fields.STORED_SOURCE).unwrap(),
        parser.getField(Fields.NESTED_OPTIONS).unwrap());
  }

  @Override
  public Type getVectorSearchType() {
    return query().map(ignored -> Type.AUTO_EMBEDDING).orElse(Type.EXACT);
  }

  @Override
  public VectorSearchCriteria withQueryVector(Vector vector) {
    return new ExactVectorSearchCriteria(
        this.path,
        Optional.of(vector),
        Optional.empty(),
        this.filter,
        this.parentFilter,
        this.limit,
        this.returnStoredSource,
        this.embeddedOptions);
  }

  @Override
  public VectorSearchCriteria withPath(FieldPath path) {
    return new ExactVectorSearchCriteria(
        path,
        this.queryVector,
        this.query,
        this.filter,
        this.parentFilter,
        this.limit,
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
            .field(Fields.EXACT, true)
            .field(Fields.QUERY_VECTOR, this.queryVector())
            .join(queryDocument)
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
