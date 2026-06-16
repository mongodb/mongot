package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class SortStatsBuilder {
  private Optional<QueryExecutionArea> profilingIteratorExecutionArea;
  private Optional<QueryExecutionArea> sortExecutionArea;
  private Optional<Map<String, List<FieldName.TypeField>>> fieldInfos;
  private Optional<Boolean> usesIndexSort = Optional.empty();

  public static SortStatsBuilder builder() {
    return new SortStatsBuilder();
  }

  public SortStatsBuilder profilingIteratorExecutionArea(
      QueryExecutionArea profilingIteratorExecutionArea) {
    this.profilingIteratorExecutionArea = Optional.of(profilingIteratorExecutionArea);
    return this;
  }

  public SortStatsBuilder sortExecutionArea(QueryExecutionArea queryExecutionArea) {
    this.sortExecutionArea = Optional.of(queryExecutionArea);
    return this;
  }

  public SortStatsBuilder fieldInfos(Map<String, List<FieldName.TypeField>> fieldInfos) {
    this.fieldInfos = Optional.of(fieldInfos);
    return this;
  }

  public SortStatsBuilder usesIndexSort(boolean usesIndexSort) {
    this.usesIndexSort = Optional.of(usesIndexSort);
    return this;
  }

  public SortStats build() {
    var infos = Check.isPresent(this.fieldInfos, "fieldInfos");
    Optional<SortStats.SortExplainTimingBreakdown> sortExplainTimingBreakdown =
        Stream.of(this.profilingIteratorExecutionArea, this.sortExecutionArea)
                .allMatch(Optional::isPresent)
            ? Optional.of(
                new SortStats.SortExplainTimingBreakdown(
                    this.profilingIteratorExecutionArea.get(), this.sortExecutionArea.get()))
            : Optional.empty();

    return new SortStats(sortExplainTimingBreakdown, infos, this.usesIndexSort);
  }
}
