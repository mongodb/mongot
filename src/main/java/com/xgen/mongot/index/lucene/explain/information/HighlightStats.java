package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.query.highlights.Highlight;
import com.xgen.mongot.util.Equality;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.bson.BsonDocument;

public record HighlightStats(
    List<String> resolvedHighlightPaths,
    Map<String, UnifiedHighlighter.OffsetSource> offsetSources,
    Optional<QueryExecutionArea> stats)
    implements DocumentEncodable,
        EqualsWithTimingEquator<HighlightStats>,
        Comparable<HighlightStats> {
  static class Fields {
    static final Field.Required<List<String>> RESOLVED_HIGHLIGHT_PATHS =
        Field.builder("resolvedHighlightPaths")
            .listOf(Value.builder().stringValue().required())
            .required();

    static final Field.Optional<QueryExecutionArea> STATS =
        Field.builder("stats")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.WithDefault<Map<String, UnifiedHighlighter.OffsetSource>> OFFSET_SOURCES =
        Field.builder("offsetSources")
            .enumField(UnifiedHighlighter.OffsetSource.class)
            .asCamelCase()
            .asMap()
            .optional()
            .withDefault(Map.of());
  }

  public static HighlightStats create(
      Highlight highlight,
      Map<String, UnifiedHighlighter.OffsetSource> offsetSources,
      Optional<QueryExecutionArea> stats) {
    return new HighlightStats(highlight.resolvedLuceneFieldNames(), offsetSources, stats);
  }

  public static HighlightStats fromBson(DocumentParser parser) throws BsonParseException {
    return new HighlightStats(
        parser.getField(Fields.RESOLVED_HIGHLIGHT_PATHS).unwrap(),
        parser.getField(Fields.OFFSET_SOURCES).unwrap(),
        parser.getField(Fields.STATS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.RESOLVED_HIGHLIGHT_PATHS, this.resolvedHighlightPaths)
        .field(Fields.STATS, this.stats)
        .field(Fields.OFFSET_SOURCES, this.offsetSources)
        .build();
  }

  @Override
  public boolean equals(HighlightStats other, Equator<QueryExecutionArea> timingEquator) {
    return this.offsetSources.equals(other.offsetSources)
        && Equality.equals(this.stats, other.stats, timingEquator)
        && this.resolvedHighlightPaths.stream()
            .sorted()
            .toList()
            .equals(other.resolvedHighlightPaths.stream().sorted().toList());
  }

  @Override
  public int compareTo(HighlightStats other) {
    int pathSizeComparison =
        Integer.compare(this.resolvedHighlightPaths.size(), other.resolvedHighlightPaths.size());
    if (pathSizeComparison != 0) {
      return pathSizeComparison;
    }

    int offsetComparison = Integer.compare(this.offsetSources.size(), other.offsetSources.size());
    if (offsetComparison != 0) {
      return offsetComparison;
    }

    return Optionals.compareTo(this.stats, other.stats);
  }
}
