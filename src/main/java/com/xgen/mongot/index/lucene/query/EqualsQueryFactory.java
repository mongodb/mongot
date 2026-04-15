package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.lucene.util.AnalyzedText;
import com.xgen.mongot.index.lucene.util.InvalidAnalyzerOutputException;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.value.BooleanValue;
import com.xgen.mongot.index.query.operators.value.DateValue;
import com.xgen.mongot.index.query.operators.value.NullValue;
import com.xgen.mongot.index.query.operators.value.NumericValue;
import com.xgen.mongot.index.query.operators.value.ObjectIdValue;
import com.xgen.mongot.index.query.operators.value.StringValue;
import com.xgen.mongot.index.query.operators.value.UuidValue;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

class EqualsQueryFactory {

  private final QueryFactoryContext queryFactoryContext;
  private final DateRangeQueryFactory dateRangeQueryFactory;

  EqualsQueryFactory(QueryFactoryContext queryFactoryContext) {
    this(
        queryFactoryContext,
        new DateRangeQueryFactory(
            queryFactoryContext.getIndexCapabilities(), DynamicFeatureFlagRegistry.empty()));
  }

  EqualsQueryFactory(
      QueryFactoryContext queryFactoryContext, DateRangeQueryFactory dateRangeQueryFactory) {
    this.queryFactoryContext = queryFactoryContext;
    this.dateRangeQueryFactory = dateRangeQueryFactory;
  }

  Query fromValue(Value value, FieldPath path, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    return switch (value) {
      case DateValue dateValue -> dateQuery(dateValue, path, embeddedRoot);
      case NumericValue numericValue -> numericQuery(numericValue, path, embeddedRoot);
      case StringValue stringValue -> stringQuery(stringValue, path, embeddedRoot);
      case BooleanValue booleanValue -> booleanQuery(booleanValue, path, embeddedRoot);
      case ObjectIdValue objectIdValue -> objectIdQuery(objectIdValue, path, embeddedRoot);
      case UuidValue uuidValue -> uuidQuery(uuidValue, path, embeddedRoot);
      case NullValue nullValue -> nullQuery(path, embeddedRoot);
    };
  }

  private Query booleanQuery(
      BooleanValue booleanEquals, FieldPath path, Optional<FieldPath> embeddedRoot) {
    // We use a ConstantScoreQuery so that the score does not factor in the
    // tf/idf computed due to the TermQuery.
    return new ConstantScoreQuery(
        new TermQuery(
            new Term(
                FieldName.TypeField.BOOLEAN.getLuceneFieldName(path, embeddedRoot),
                booleanEquals.value()
                    ? FieldValue.BOOLEAN_TRUE_FIELD_VALUE
                    : FieldValue.BOOLEAN_FALSE_FIELD_VALUE)));
  }

  private Query objectIdQuery(
      ObjectIdValue objectIdValue, FieldPath path, Optional<FieldPath> embeddedRoot) {
    // We use a ConstantScoreQuery so that the score does not factor in the
    // tf/idf computed due to the TermQuery.
    return new ConstantScoreQuery(
        new TermQuery(
            new Term(
                FieldName.TypeField.OBJECT_ID.getLuceneFieldName(path, embeddedRoot),
                new BytesRef(objectIdValue.value().toByteArray()))));
  }

  private Query dateQuery(DateValue dateValue, FieldPath path, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    return this.dateRangeQueryFactory.fromBounds(dateValue.getDateRangeBound())
        .apply(path, embeddedRoot);
  }

  private Query numericQuery(
      NumericValue numericValue, FieldPath path, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    return NumericRangeQueryFactory.fromBounds(numericValue.getNumericRangeBound())
        .apply(path, embeddedRoot);
  }

  private Query stringQuery(
      StringValue stringValue, FieldPath path, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {

    var fieldName = FieldName.TypeField.TOKEN.getLuceneFieldName(path, embeddedRoot);
    var analyzer = this.queryFactoryContext.getTokenFieldAnalyzer();

    try {
      var analyzedToken =
          new BytesRef(
              AnalyzedText.applyTokenFieldTypeNormalizer(fieldName, analyzer, stringValue.value()));

      var indexQuery = new ConstantScoreQuery(new TermQuery(new Term(fieldName, analyzedToken)));
      var docValuesQuery = SortedSetDocValuesField.newSlowExactQuery(fieldName, analyzedToken);

      return new IndexOrDocValuesQuery(indexQuery, docValuesQuery);

    } catch (InvalidAnalyzerOutputException e) {
      throw new InvalidQueryException(
          String.format("Path '%s' needs to be indexed as token", path));
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Query uuidQuery(
      UuidValue uuidValue, FieldPath path, Optional<FieldPath> embeddedRoot) {
    String fieldName = FieldName.TypeField.UUID.getLuceneFieldName(path, embeddedRoot);
    String uuidString = uuidValue.uuid().toString();

    // We use a ConstantScoreQuery so that the score does not factor in the
    // tf/idf computed due to the TermQuery.
    var indexQuery = new ConstantScoreQuery(new TermQuery(new Term(fieldName, uuidString)));

    // Uses a ConstantScoreWeight underneath as well
    var docValuesQuery =
        SortedSetDocValuesField.newSlowExactQuery(fieldName, new BytesRef(uuidString));

    return new IndexOrDocValuesQuery(indexQuery, docValuesQuery);
  }

  private Query nullQuery(FieldPath path, Optional<FieldPath> embeddedRoot) {
    // We use a ConstantScoreQuery so that the score does not factor in the
    // tf/idf computed due to the TermQuery.
    return new ConstantScoreQuery(
        new TermQuery(
            new Term(
                FieldName.TypeField.NULL.getLuceneFieldName(path, embeddedRoot),
                FieldValue.NULL_FIELD_VALUE)));
  }
}
