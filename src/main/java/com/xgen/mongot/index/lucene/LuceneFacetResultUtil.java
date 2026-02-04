package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.definition.FacetableStringFieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.FacetFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.timers.InvocationCountingTimer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.range.LongRangeFacetCounts;

public class LuceneFacetResultUtil {

  static FacetResult getBoundaryFacetResult(
      FacetDefinition.BoundaryFacetDefinition<?> boundaryDefinition,
      LuceneFacetContext facetContext,
      FacetsCollector facetsCollector,
      Optional<FieldPath> returnScope)
      throws IOException, InvalidQueryException {
    String path = facetContext.getBoundaryFacetPath(boundaryDefinition, returnScope);
    Optional<ExplainTimings> timings =
        Explain.getQueryInfo()
            .map(
                queryInfo ->
                    queryInfo
                        .getFeatureExplainer(
                            FacetFeatureExplainer.class, FacetFeatureExplainer::new)
                        .getCreateCountTimings());

    LongRangeFacetCounts facetCounts;
    try (var unused =
        new InvocationCountingTimer.AutocloseableOptional<>(
            timings.map(t -> t.split(ExplainTimings.Type.GENERATE_FACET_COUNTS)))) {
      facetCounts =
          new LongRangeFacetCounts(
              path, facetsCollector, facetContext.getRanges(boundaryDefinition, returnScope));
    }

    return facetCounts.getAllChildren(path);
  }

  static Map<FieldTypeDefinition.Type, Map<String, FacetDefinition.StringFacetDefinition>>
      groupFacetableStringDefinitions(
          LuceneFacetContext facetContext,
          Map<String, FacetDefinition> stringFacetDefinitions,
          Optional<FieldPath> returnScope)
          throws InvalidQueryException {

    Map<FieldTypeDefinition.Type, Map<String, FacetDefinition.StringFacetDefinition>> result =
        new HashMap<>();

    FacetableStringFieldDefinition.TYPES.forEach(type -> result.put(type, new HashMap<>()));

    for (Map.Entry<String, FacetDefinition> entry : stringFacetDefinitions.entrySet()) {
      String facetName = entry.getKey();
      FacetDefinition facetDefinition = entry.getValue();

      if (facetDefinition.getType() != FacetDefinition.Type.STRING) {
        continue;
      }
      FacetDefinition.StringFacetDefinition stringFacetDefinition =
          (FacetDefinition.StringFacetDefinition) facetDefinition;
      FieldTypeDefinition.Type type =
          facetContext.getStringFacetFieldDefinition(stringFacetDefinition, returnScope).getType();

      result.get(type).put(facetName, stringFacetDefinition);
    }
    return result;
  }

  static CountResult getCount(long count, Count.Type countType) {
    return switch (countType) {
      case TOTAL -> CountResult.totalCount(count);
      case LOWER_BOUND -> CountResult.lowerBoundCount(count);
    };
  }

  static Optional<FacetFeatureExplainer> getFacetFeatureExplainer() {
    return Explain.getQueryInfo()
        .map(
            queryInfo ->
                queryInfo.getFeatureExplainer(
                    FacetFeatureExplainer.class, FacetFeatureExplainer::new));
  }
}
