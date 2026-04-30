package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record DisableBulkScorerQuerySpec(QueryExplainInformation query)
    implements LuceneQuerySpecification {

  static class Fields {
    static final Field.Required<QueryExplainInformation> QUERY =
        Field.builder("query")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .required();
  }

  @Override
  public Type getType() {
    return Type.DISABLE_BULK_SCORER_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    return other.getType() == getType()
        && explainInfoEquator.equate(
            this.query, ((DisableBulkScorerQuerySpec) other).query);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.QUERY, this.query).build();
  }

  static DisableBulkScorerQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new DisableBulkScorerQuerySpec(parser.getField(Fields.QUERY).unwrap());
  }
}
