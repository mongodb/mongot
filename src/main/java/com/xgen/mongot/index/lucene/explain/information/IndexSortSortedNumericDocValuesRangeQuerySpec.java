package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

/**
 * Explain spec for Lucene's {@code IndexSortSortedNumericDocValuesRangeQuery}, the query that
 * performs the index-sort range optimization. Without this spec the query has no {@link
 * LuceneQuerySpecification.Type} entry and falls through to {@link DefaultQuerySpec}, rendering as
 * an opaque {@code DefaultQuery} node in the middle of the range query's subtree.
 *
 * <p>Only {@code path} is emitted. The range bounds are deliberately omitted: they are already
 * shown on the sibling fallback subtree's {@code PointRangeQuery} node, and the raw {@code
 * lowerValue()}/{@code upperValue()} longs on this query are type-dependently encoded (raw long for
 * {@code NUMBER_INT64_V2}/{@code DATE_V2}, {@code toMqlSortableLong} for {@code NUMBER_DOUBLE_V2}),
 * so rendering them correctly requires the same per-type decoding {@code
 * PointRangeQuerySpecCreator} needs.
 */
public record IndexSortSortedNumericDocValuesRangeQuerySpec(FieldPath path)
    implements LuceneQuerySpecification {

  static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();
  }

  @Override
  public Type getType() {
    return Type.INDEX_SORT_SORTED_NUMERIC_DOC_VALUES_RANGE_QUERY;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.PATH, this.path).build();
  }

  static IndexSortSortedNumericDocValuesRangeQuerySpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new IndexSortSortedNumericDocValuesRangeQuerySpec(parser.getField(Fields.PATH).unwrap());
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }
    return Objects.equals(this.path, ((IndexSortSortedNumericDocValuesRangeQuerySpec) other).path);
  }
}
