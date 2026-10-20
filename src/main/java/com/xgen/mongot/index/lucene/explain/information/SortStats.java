package com.xgen.mongot.index.lucene.explain.information;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSetMultimap;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record SortStats(
    Optional<SortExplainTimingBreakdown> stats,
    Map<String, List<FieldName.TypeField>> fieldInfos,
    Optional<Boolean> usesIndexSort,
    Optional<BsonDocument> embeddedDocuments)
    implements DocumentEncodable, Comparable<SortStats> {
  static class Fields {
    static final Field.Optional<SortExplainTimingBreakdown> STATS =
        Field.builder("stats")
            .classField(SortExplainTimingBreakdown::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    /**
     * Maps each scoped sort field to {@code {path, query}}, where query is the standard structural
     * child-query explain tree. Absent when no sort key is scoped.
     */
    static final Field.Optional<BsonDocument> EMBEDDED_DOCUMENTS =
        Field.builder("embeddedDocuments").documentField().optional().noDefault();

    static final Field.Required<Map<String, List<FieldName.TypeField>>> FIELD_INFOS =
        Field.builder("fieldInfos")
            .mapOf(
                Value.builder()
                    .listOf(
                        Value.builder()
                            .enumValue(FieldName.TypeField.class)
                            .asCamelCase()
                            .required())
                    .required())
            .required();

    static final Field.Optional<Boolean> USES_INDEX_SORT =
        Field.builder("usesIndexSort").booleanField().optional().noDefault();
  }

  public static SortStats create(
      ImmutableSetMultimap<FieldPath, FieldName.TypeField> fieldToTypeField,
      Optional<Boolean> usesIndexSort,
      Optional<BsonDocument> embeddedDocuments) {
    return SortStats.create(
        Optional.empty(), Optional.empty(), fieldToTypeField, usesIndexSort, embeddedDocuments);
  }

  public static SortStats create(
      Optional<QueryExecutionArea> prunedResultIterator,
      Optional<QueryExecutionArea> comparator,
      ImmutableSetMultimap<FieldPath, FieldName.TypeField> fieldToTypeField,
      Optional<Boolean> usesIndexSort,
      Optional<BsonDocument> embeddedDocuments) {

    Optional<SortExplainTimingBreakdown> sortExplainTimingBreakdown =
        Stream.of(prunedResultIterator, comparator).allMatch(Optional::isPresent)
            ? Optional.of(
                new SortExplainTimingBreakdown(prunedResultIterator.get(), comparator.get()))
            : Optional.empty();

    var fieldInfos =
        fieldToTypeField.entries().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    entry -> entry.getKey().toString(),
                    entry -> List.of(entry.getValue()),
                    (prevEntry, newEntry) ->
                        Stream.concat(prevEntry.stream(), newEntry.stream())
                            // sort list to be able to deterministically compare 2 fieldInfos
                            .sorted()
                            .collect(Collectors.toList())));

    return new SortStats(sortExplainTimingBreakdown, fieldInfos, usesIndexSort, embeddedDocuments);
  }

  public static SortStats fromBson(DocumentParser parser) throws BsonParseException {
    return new SortStats(
        parser.getField(Fields.STATS).unwrap(),
        parser.getField(Fields.FIELD_INFOS).unwrap(),
        parser.getField(Fields.USES_INDEX_SORT).unwrap(),
        parser.getField(Fields.EMBEDDED_DOCUMENTS).unwrap());
  }

  public boolean equals(SortStats other, Equator<QueryExecutionArea> timingEquator) {
    return this.stats
            .flatMap(
                presentStats ->
                    other.stats.map(otherStats -> presentStats.equals(otherStats, timingEquator)))
            .orElse(true)
        // both stats are either present or absent
        && Objects.equals(this.fieldInfos, other.fieldInfos)
        && Objects.equals(this.usesIndexSort, other.usesIndexSort)
        && Objects.equals(this.embeddedDocuments, other.embeddedDocuments);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.FIELD_INFOS, this.fieldInfos)
        .field(Fields.STATS, this.stats)
        .field(Fields.USES_INDEX_SORT, this.usesIndexSort)
        .field(Fields.EMBEDDED_DOCUMENTS, this.embeddedDocuments)
        .build();
  }

  @Override
  public int compareTo(SortStats other) {
    // not meaningful to compare fieldInfos
    return Optionals.compareTo(this.stats, other.stats);
  }

  @VisibleForTesting
  public record SortExplainTimingBreakdown(
      QueryExecutionArea prunedResultIterator, QueryExecutionArea comparator)
      implements DocumentEncodable, Comparable<SortExplainTimingBreakdown> {
    static class Fields {
      static final Field.Required<QueryExecutionArea> PRUNED_RESULT_ITERATOR =
          Field.builder("prunedResultIterator")
              .classField(QueryExecutionArea::fromBson)
              .disallowUnknownFields()
              .required();

      static final Field.Required<QueryExecutionArea> COMPARATOR =
          Field.builder("comparator")
              .classField(QueryExecutionArea::fromBson)
              .disallowUnknownFields()
              .required();
    }

    public static SortExplainTimingBreakdown fromBson(DocumentParser parser)
        throws BsonParseException {
      return new SortExplainTimingBreakdown(
          parser.getField(Fields.PRUNED_RESULT_ITERATOR).unwrap(),
          parser.getField(Fields.COMPARATOR).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.PRUNED_RESULT_ITERATOR, this.prunedResultIterator)
          .field(Fields.COMPARATOR, this.comparator)
          .build();
    }

    @Override
    public int compareTo(SortStats.SortExplainTimingBreakdown o) {
      int prunedIteratorComparison = this.prunedResultIterator.compareTo(o.prunedResultIterator);
      if (prunedIteratorComparison != 0) {
        return prunedIteratorComparison;
      }

      return this.comparator.compareTo(o.comparator);
    }

    public boolean equals(
        SortExplainTimingBreakdown other, Equator<QueryExecutionArea> timingEquator) {
      return timingEquator.equate(this.prunedResultIterator, other.prunedResultIterator)
          && timingEquator.equate(this.comparator, other.comparator);
    }
  }
}
