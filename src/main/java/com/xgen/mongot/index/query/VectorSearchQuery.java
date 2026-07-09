package com.xgen.mongot.index.query;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Optional;
import org.bson.BsonDocument;

// TODO(CLOUDP-368066): The userReturnStoredSource and vectorStoredSourceEnabled fields will be
// removed and replaced by a single returnStoredSource parameter. The returnStoredSource setting
// was always accepted here, but was silently ignored, so we are doing some gymnastics here for
// now while giving users time to fix their vector indexes. Only the name returnStoredSource is
// visible to users (see Fields.STORED_SOURCE, below).
public record VectorSearchQuery(
    String index,
    VectorSearchCriteria criteria,
    boolean userReturnStoredSource, // NOTE(corecursion): this field is not visible to users
    boolean vectorStoredSourceEnabled, // NOTE(corecursion): this field is not visible to users
    // Per-query override for Lucene concurrent segment search; absent means "engine default".
    Optional<Boolean> concurrentOverride)
    implements Query {

  public VectorSearchQuery {
    Check.argNotEmpty(index, "index");
  }

  static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();

    static final Field.Required<String> INDEX =
        Field.builder("index").stringField().mustNotBeEmpty().required();

    static final Field.WithDefault<Boolean> STORED_SOURCE =
        Field.builder("returnStoredSource").booleanField().optional().withDefault(false);

    static final Field.Optional<Boolean> CONCURRENT =
        Field.builder("concurrent").booleanField().optional().noDefault();
  }

  public static VectorSearchQuery fromBson(DocumentParser parser, boolean vectorStoredSourceEnabled)
      throws BsonParseException {
    // Don't care about the value, just want to validate deserialization
    parser.getField(Query.Fields.SEARCH_NODE_PREFERENCE).unwrap();

    return new VectorSearchQuery(
        parser.getField(Fields.INDEX).unwrap(),
        VectorSearchCriteria.fromBson(
            parser, parser.getField(Fields.PATH).unwrap(), VectorSearchFilter.Type.CLAUSE),
        parser.getField(Fields.STORED_SOURCE).unwrap(), // userReturnStoredSource!
        vectorStoredSourceEnabled,
        parser.getField(Fields.CONCURRENT).unwrap());
  }

  // Only used for testing.
  @VisibleForTesting
  public static VectorSearchQuery fromBson(BsonDocument document) throws BsonParseException {
    try (var guard = Tracing.simpleSpanGuard("VectorSearchQuery.fromBson")) {
      try (var parser = BsonDocumentParser.withContext(BsonParseContext.root(), document).build()) {
        return fromBson(parser, true);
      }
    }
  }

  @Override
  public BsonDocument toBson() {
    BsonDocument criteriaBson = this.criteria.toBson();
    return BsonDocumentBuilder.builder()
        .field(Fields.INDEX, this.index())
        .field(Fields.PATH, this.criteria().path())
        .field(Fields.STORED_SOURCE, this.userReturnStoredSource()) // TODO(CLOUDP-368066) see above
        .field(Fields.CONCURRENT, this.concurrentOverride())
        .join(criteriaBson)
        .build();
  }

  @Override
  public boolean concurrent() {
    return this.concurrentOverride.orElse(true);
  }

  @Override
  public boolean returnStoredSource() { // TODO(CLOUDP-368066) see above
    return this.vectorStoredSourceEnabled && this.userReturnStoredSource;
  }

  @Override
  public Optional<ReturnScope> returnScope() {
    return Optional.empty();
  }
}
