package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.util.AnalyzedText;
import com.xgen.mongot.index.lucene.util.InvalidAnalyzerOutputException;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.value.DateValue;
import com.xgen.mongot.index.query.operators.value.NonNullValue;
import com.xgen.mongot.index.query.operators.value.NumericValue;
import com.xgen.mongot.index.query.operators.value.StringValue;
import com.xgen.mongot.index.query.operators.value.UuidValue;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.bson.types.ObjectId;

class InQueryFactory {

  private final QueryFactoryContext factoryContext;

  InQueryFactory(QueryFactoryContext factoryContext) {
    this.factoryContext = factoryContext;
  }

  public Query fromValues(
      List<NonNullValue> values,
      NonNullValueType type,
      List<FieldPath> paths,
      SingleQueryContext queryContext)
      throws InvalidQueryException {

    return switch (type) {
      case DATE -> dateQuery(mapValues(values, NonNullValue::asDate), paths, queryContext);
      case NUMBER -> numericQuery(mapValues(values, NonNullValue::asNumber), paths, queryContext);
      case STRING -> stringQuery(mapValues(values, NonNullValue::asString), paths, queryContext);
      case BOOLEAN ->
          booleanQuery(mapValues(values, value -> value.asBoolean().value()), paths,
              queryContext);
      case OBJECT_ID ->
          objectIdQuery(mapValues(values, value -> value.asObjectId().value()), paths,
              queryContext);
      case UUID -> uuidQuery(mapValues(values, NonNullValue::asUuid), paths, queryContext);
    };
  }

  private Query booleanQuery(
      List<Boolean> values, List<FieldPath> paths, SingleQueryContext singleQueryContext) {
    // Build all per-path queries
    List<Query> queries =
        paths.stream()
            .flatMap(
                path -> {
                  var fieldName =
                      TypeField.BOOLEAN.getLuceneFieldName(
                          path, singleQueryContext.getEmbeddedRoot());
                  return values.stream()
                      .distinct()
                      .map(
                          value ->
                              value
                                  ? FieldValue.BOOLEAN_TRUE_FIELD_VALUE
                                  : FieldValue.BOOLEAN_FALSE_FIELD_VALUE)
                      .map(value -> (Query) new TermQuery(new Term(fieldName, value)));
                })
            .toList();
    // Combine all queries with a disjunction using static BooleanComposer
    return new ConstantScoreQuery(BooleanComposer.should(queries.toArray(new Query[0])));
  }

  private Query objectIdQuery(
      List<ObjectId> values, List<FieldPath> paths, SingleQueryContext singleQueryContext) {
    // Build all per-path TermQuery objects
    List<Query> queries =
        paths.stream()
            .flatMap(
                path -> {
                  var fieldName =
                      TypeField.OBJECT_ID.getLuceneFieldName(
                          path, singleQueryContext.getEmbeddedRoot());
                  return values.stream()
                      .map(
                          value ->
                              (Query)
                                  new TermQuery(
                                      new Term(
                                          fieldName,
                                          new BytesRef(value.toByteArray())))); // cast to Query
                })
            .toList();
    // Combine all queries using the static BooleanComposer.should(...) utility
    return new ConstantScoreQuery(BooleanComposer.should(queries.toArray(new Query[0])));
  }

  private Query dateQuery(
      List<DateValue> dateValues, List<FieldPath> paths, SingleQueryContext singleQueryContext) {
    // Convert all date values to milliseconds
    long[] dates = dateValues.stream().mapToLong(date -> date.point().value().getTime()).toArray();
    // Build all per-path queries (no inner ConstantScoreQuery)
    List<Query> queries =
        paths.stream()
            .flatMap(
                path -> {
                  var dateField =
                      TypeField.DATE.getLuceneFieldName(path, singleQueryContext.getEmbeddedRoot());
                  var dateMultipleField =
                      TypeField.DATE_MULTIPLE.getLuceneFieldName(
                          path, singleQueryContext.getEmbeddedRoot());

                  Query singleValueQuery = LongPoint.newSetQuery(dateField, dates);
                  Query multiValueQuery = LongPoint.newSetQuery(dateMultipleField, dates);

                  return Stream.of(singleValueQuery, multiValueQuery);
                })
            .toList();
    // Wrap final disjunction in ConstantScoreQuery
    return new ConstantScoreQuery(BooleanComposer.should(queries.toArray(new Query[0])));
  }

  private Query numericQuery(
      List<NumericValue> values, List<FieldPath> paths, SingleQueryContext singleQueryContext) {

    long[] longs =
        values.stream().mapToLong(p -> p.asNumber().point().getLongValueRepresentation()).toArray();
    long[] doubles =
        values.stream()
            .mapToLong(p -> p.asNumber().point().getDoubleValueRepresentation())
            .toArray();
    Optional<FieldPath> embeddedRoot = singleQueryContext.getEmbeddedRoot();

    List<Query> queries =
        paths.stream()
            .flatMap(
                path -> {
                  Query numberInt64Query =
                      this.factoryContext.getFeatureFlags().isEnabled(
                              Feature.INDEX_OR_DOC_VALUES_QUERY_FOR_IN_OPERATOR)
                          ? LongField.newSetQuery(
                              TypeField.NUMBER_INT64.getLuceneFieldName(path, embeddedRoot), longs)
                          : LongPoint.newSetQuery(
                              TypeField.NUMBER_INT64.getLuceneFieldName(path, embeddedRoot), longs);

                  Query numberInt64MultipleQuery =
                      LongPoint.newSetQuery(
                          TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(path, embeddedRoot),
                          longs);

                  Query numberDoubleQuery =
                      this.factoryContext.getFeatureFlags().isEnabled(
                              Feature.INDEX_OR_DOC_VALUES_QUERY_FOR_IN_OPERATOR)
                          ? LongField.newSetQuery(
                              TypeField.NUMBER_DOUBLE.getLuceneFieldName(path, embeddedRoot),
                              doubles)
                          : LongPoint.newSetQuery(
                              TypeField.NUMBER_DOUBLE.getLuceneFieldName(path, embeddedRoot),
                              doubles);

                  Query numberDoubleMultipleQuery =
                      LongPoint.newSetQuery(
                          TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(path, embeddedRoot),
                          doubles);

                  return Stream.of(
                      numberInt64Query,
                      numberInt64MultipleQuery,
                      numberDoubleQuery,
                      numberDoubleMultipleQuery);
                })
            .toList();

    return new ConstantScoreQuery(BooleanComposer.should(queries.toArray(new Query[0])));
  }

  private Query stringQuery(
      List<StringValue> stringValues, List<FieldPath> paths, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {

    var analyzer = this.factoryContext.getTokenFieldAnalyzer();

    // Build queries for each path
    List<Query> queries =
        CheckedStream.from(paths.stream())
            .mapAndCollectChecked(
                path -> {
                  String field =
                      TypeField.TOKEN.getLuceneFieldName(
                          path, singleQueryContext.getEmbeddedRoot());
                  try {
                    List<BytesRef> analyzedStrings =
                        CheckedStream.from(stringValues.stream())
                            .mapAndCollectChecked(
                                string ->
                                    new BytesRef(
                                        AnalyzedText.applyTokenFieldTypeNormalizer(
                                            field, analyzer, string.value())));

                    var indexQuery = new TermInSetQuery(field, analyzedStrings);
                    var docValuesQuery =
                        SortedSetDocValuesField.newSlowSetQuery(field, analyzedStrings);
                    return (Query) new IndexOrDocValuesQuery(indexQuery, docValuesQuery);

                  } catch (InvalidAnalyzerOutputException e) {
                    throw new InvalidQueryException(
                        String.format("Path '%s' needs to be indexed as token", path));
                  } catch (IOException e) {
                    throw new IllegalStateException(e);
                  }
                });

    if (queries.size() == 1) {
      return queries.get(0);
    }
    return new ConstantScoreQuery(BooleanComposer.should(queries.toArray(new Query[0])));
  }

  private static Query uuidQuery(
      List<UuidValue> uuidValues, List<FieldPath> paths, SingleQueryContext singleQueryContext) {
    // Convert UUIDs to BytesRef
    List<BytesRef> uuids =
        uuidValues.stream().map(UuidValue::uuid).map(UUID::toString).map(BytesRef::new).toList();
    // Build queries for each path
    List<Query> queries =
        paths.stream()
            .map(
                fieldPath -> {
                  String fieldName =
                      TypeField.UUID.getLuceneFieldName(
                          fieldPath, singleQueryContext.getEmbeddedRoot());
                  var indexQuery = new TermInSetQuery(fieldName, uuids);
                  var docValuesQuery = SortedSetDocValuesField.newSlowSetQuery(fieldName, uuids);
                  return (Query) new IndexOrDocValuesQuery(indexQuery, docValuesQuery);
                })
            .toList();

    if (queries.size() == 1) {
      // Return the single query directly (to match expected test output)
      return queries.get(0);
    }
    // Multiple paths → combine under ConstantScore(BooleanQuery.should(...))
    return new ConstantScoreQuery(BooleanComposer.should(queries.toArray(new Query[0])));
  }

  private <R> List<R> mapValues(List<NonNullValue> values, Function<NonNullValue, R> mapper) {
    return values.stream().map(mapper).collect(Collectors.toList());
  }
}
