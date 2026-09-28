package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.IndexSortSortedNumericDocValuesRangeQuerySpec;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;

public class IndexSortSortedNumericDocValuesRangeQuerySpecCreator {
  static IndexSortSortedNumericDocValuesRangeQuerySpec fromQuery(
      IndexSortSortedNumericDocValuesRangeQuery query) {
    return new IndexSortSortedNumericDocValuesRangeQuerySpec(
        FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(query.getField())));
  }
}
