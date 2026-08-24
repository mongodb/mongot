package com.xgen.mongot.config.manager;

import java.util.Arrays;

public enum IndexChangeReason {
  FIELDS("vector fields or filters have changed"),
  NUM_PARTITIONS("numPartitions has changed"),
  DEFINITION_VERSION("definitionVersion has changed"),
  AUTO_EMBEDDING_DEFINITION_VERSION("autoEmbeddingDefinitionVersion has changed"),
  VIEW("view has changed"),
  INDEX_FEATURE_VERSION("indexFeatureVersion has changed"),
  ANALYZER("analyzer has changed"),
  SEARCH_ANALYZER("searchAnalyzer has changed"),
  MAPPINGS("mappings have changed"),
  ANALYZERS("analyzers have changed"),
  SYNONYMS("synonyms have changed"),
  STORED_SOURCE("storedSource has changed"),

  // catch-all; specific modified analyzers would fall into this category
  OTHER("definition has changed");

  private final String description;

  IndexChangeReason(String description) {
    this.description = description;
  }

  public String getDescription() {
    return this.description;
  }

  public static IndexChangeReason findByDescription(String val) {
    return Arrays.stream(values())
        .filter(value -> value.getDescription().equals(val))
        .findFirst()
        .orElse(OTHER);
  }
}
