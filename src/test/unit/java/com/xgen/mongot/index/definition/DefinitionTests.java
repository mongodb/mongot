package com.xgen.mongot.index.definition;

import com.xgen.testing.mongot.index.definition.EmbeddingModelCatalogTestUtils;

class DefinitionTests {
  static final String RESOURCES_PATH = initialize();

  private static String initialize() {
    EmbeddingModelCatalogTestUtils.registerAutoEmbedModel("voyage-3-large");
    EmbeddingModelCatalogTestUtils.registerAutoEmbedModel("voyage-4-lite");
    return "src/test/unit/resources/index/definition/";
  }
}
