package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

/**
 * Explain spec for {@code NullnessAwareIndexSortRangeQuery}.
 *
 * <p>{@code useIndexSort} reports whether the index-sort fast path was detected speeding up the
 * range query on at least one segment. It is best-effort and asymmetric: {@code true} means the
 * fast path definitely ran, {@code false} means it was not detected and does not prove that it did
 * not run. Lucene exposes no direct signal, so detection compares the optimized scorer's cost
 * against the fallback's; equal costs (for example a range spanning a whole segment) read as {@code
 * false}. Treat {@code false} as "no evidence" rather than "did not happen".
 */
public record NullnessAwareIndexSortRangeQuerySpec(
    boolean useIndexSort, List<QueryExplainInformation> children)
    implements LuceneQuerySpecification {

  static class Fields {
    static final Field.Required<Boolean> USE_INDEX_SORT =
        Field.builder("useIndexSort").booleanField().required();

    static final Field.Required<List<QueryExplainInformation>> QUERY =
        Field.builder("query")
            .listOf(
                Value.builder()
                    .classValue(QueryExplainInformation::fromBson)
                    .disallowUnknownFields()
                    .required())
            .required();
  }

  @Override
  public Type getType() {
    return Type.NULLNESS_AWARE_INDEX_SORT_RANGE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != this.getType()) {
      return false;
    }

    NullnessAwareIndexSortRangeQuerySpec that =
        Check.instanceOf(other, NullnessAwareIndexSortRangeQuerySpec.class);
    if (that.useIndexSort != this.useIndexSort || this.children.size() != that.children.size()) {
      return false;
    }

    var theseSorted = this.children.stream().sorted(childSorter).toList();
    var thoseSorted = that.children.stream().sorted(childSorter).toList();
    for (int i = 0; i < theseSorted.size(); i++) {
      if (!explainInfoEquator.equate(theseSorted.get(i), thoseSorted.get(i))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.USE_INDEX_SORT, this.useIndexSort)
        .field(Fields.QUERY, this.children)
        .build();
  }

  static NullnessAwareIndexSortRangeQuerySpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new NullnessAwareIndexSortRangeQuerySpec(
        parser.getField(Fields.USE_INDEX_SORT).unwrap(), parser.getField(Fields.QUERY).unwrap());
  }
}
