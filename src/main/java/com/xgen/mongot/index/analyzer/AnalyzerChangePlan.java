package com.xgen.mongot.index.analyzer;

import com.google.common.collect.Sets;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CollectionUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * AnalyzerChangePlan contains information about the difference between a set of desired and
 * existing AnalyzerDefinitions.
 */
public class AnalyzerChangePlan {

  /**
   * Returns a mapping from the name of a modified analyzer definition to the desired one for all
   * analyzers that exist in both the desired set and the current AnalyzerRegistry, but whose
   * definitions differ.
   */
  public static Map<String, OverriddenBaseAnalyzerDefinition> modifiedOverridenAnalyzers(
      List<OverriddenBaseAnalyzerDefinition> existingAnalyzers,
      List<OverriddenBaseAnalyzerDefinition> desiredOverriddenDefinitions) {
    Check.elementAttributesAreUnique(
        existingAnalyzers, OverriddenBaseAnalyzerDefinition::name, "existingAnalyzers", "name");

    Check.elementAttributesAreUnique(
        desiredOverriddenDefinitions,
        OverriddenBaseAnalyzerDefinition::name,
        "desiredOverriddenDefinitions",
        "name");

    Map<String, OverriddenBaseAnalyzerDefinition> desiredDefinitionsByName =
        desiredOverriddenDefinitions.stream()
            .collect(
                CollectionUtils.toMapUnsafe(
                    OverriddenBaseAnalyzerDefinition::name, Function.identity()));
    Map<String, OverriddenBaseAnalyzerDefinition> existingDefinitionsByName =
        existingAnalyzers.stream()
            .collect(
                CollectionUtils.toMapUnsafe(
                    OverriddenBaseAnalyzerDefinition::name, Function.identity()));

    Set<String> desiredNames = desiredDefinitionsByName.keySet();
    Set<String> existingNames = existingDefinitionsByName.keySet();

    // For each of the OverriddenBaseAnalyzerDefinitions that are in both the desired set and the
    // AnalyzerRegistry, see whether the desired OverriddenBaseAnalyzerDefinition is the same as the
    // existing
    // one.
    Sets.SetView<String> overlappingNames = Sets.intersection(desiredNames, existingNames);
    Map<String, OverriddenBaseAnalyzerDefinition> modified = new HashMap<>();

    for (String overlappingName : overlappingNames) {
      OverriddenBaseAnalyzerDefinition existing = existingDefinitionsByName.get(overlappingName);
      OverriddenBaseAnalyzerDefinition desired = desiredDefinitionsByName.get(overlappingName);

      if (!existing.equals(desired)) {
        modified.put(overlappingName, desired);
      }
    }

    return modified;
  }
}
