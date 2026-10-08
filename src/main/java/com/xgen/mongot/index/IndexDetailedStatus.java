package com.xgen.mongot.index;

import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.synonym.SynonymDetailedStatus;
import com.xgen.mongot.index.version.GenerationId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public sealed interface IndexDetailedStatus
    permits IndexDetailedStatus.Search, IndexDetailedStatus.Vector {
  record Search(
      Map<String, SynonymDetailedStatus> synonymStatusMap,
      SearchIndexDefinition definition,
      List<OverriddenBaseAnalyzerDefinition> analyzers,
      IndexStatus indexStatus,
      GenerationId generationId,
      Optional<AggregatedIndexMetrics> indexMetrics)
      implements IndexDetailedStatus {}

  record Vector(
      VectorIndexDefinition definition,
      IndexStatus indexStatus,
      GenerationId generationId,
      Optional<AggregatedIndexMetrics> indexMetrics)
      implements IndexDetailedStatus {}

  IndexStatus indexStatus();

  IndexDefinition definition();

  Optional<AggregatedIndexMetrics> indexMetrics();

  GenerationId generationId();
}
