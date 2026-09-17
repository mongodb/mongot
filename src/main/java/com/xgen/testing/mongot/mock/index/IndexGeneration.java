package com.xgen.testing.mongot.mock.index;

import static com.xgen.testing.mongot.mock.index.SearchIndex.mockIndex;

import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import java.util.Collections;
import java.util.List;
import org.bson.types.ObjectId;

public class IndexGeneration {

  /** Returns an IndexGeneration that can be used in tests that require one. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration() {
    return new com.xgen.mongot.index.IndexGeneration(
        mockIndex(SearchIndex.MOCK_INDEX_DEFINITION), SearchIndex.MOCK_INDEX_DEFINITION_GENERATION);
  }

  /** Returns an IndexGeneration for input user version number. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration(
      ObjectId indexId, int userVersionNumber) {
    GenerationId generationId =
        new GenerationId(
            indexId,
            new Generation(new UserIndexVersion(userVersionNumber), IndexFormatVersion.CURRENT));
    SearchIndexDefinitionGeneration definitionGeneration = mockDefinitionGeneration(generationId);
    return new com.xgen.mongot.index.IndexGeneration(
        mockIndex(definitionGeneration), definitionGeneration);
  }

  /** Returns an IndexGeneration that can be used in tests that require one. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration(ObjectId indexId) {
    return mockIndexGeneration(SearchIndex.mockSearchDefinition(indexId));
  }

  /** Returns an IndexGeneration that can be used in tests that require one. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration(
      SearchIndexDefinition definition) {
    return mockIndexGeneration(mockDefinitionGeneration(definition));
  }

  /** Returns an IndexGeneration that can be used in tests that require one. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration(
      VectorIndexDefinition definition) {
    return mockIndexGeneration(mockDefinitionGeneration(definition));
  }

  /** Returns an IndexGeneration that can be used in tests that require one. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration(
      SearchIndexDefinition definition, List<OverriddenBaseAnalyzerDefinition> analyzers) {
    var definitionGeneration = mockDefinitionGeneration(definition, analyzers);
    return mockIndexGeneration(definitionGeneration);
  }

  /** Returns an IndexGeneration that can be used in tests that require one. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration(
      SearchIndexDefinitionGeneration definitionGeneration) {
    return new com.xgen.mongot.index.IndexGeneration(
        SearchIndex.mockIndex(definitionGeneration), definitionGeneration);
  }

  /** Returns an IndexGeneration that can be used in tests that require one. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration(
      VectorIndexDefinitionGeneration definitionGeneration) {
    return new com.xgen.mongot.index.IndexGeneration(
        VectorIndex.mockIndex(definitionGeneration), definitionGeneration);
  }

  /** Returns an IndexGeneration that can be used in tests that require one. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration(
      com.xgen.mongot.index.SearchIndex index) {
    var definitionGeneration =
        mockDefinitionGeneration(
            index.getDefinition().asSearchDefinition(), Collections.emptyList());
    return new com.xgen.mongot.index.IndexGeneration(index, definitionGeneration);
  }

  /** Returns an IndexGeneration that can be used in tests that require one. */
  public static com.xgen.mongot.index.IndexGeneration mockIndexGeneration(
      GenerationId generationId) {
    var definition = mockDefinitionGeneration(generationId);
    return mockIndexGeneration(definition);
  }

  /**
   * Returns an IndexGeneration with an InitializedIndex that can be used in tests that require one
   */
  public static com.xgen.mongot.index.IndexGeneration mockIntializedIndexGeneration(
      SearchIndexDefinitionGeneration definitionGeneration) {
    return new com.xgen.mongot.index.IndexGeneration(
        SearchIndex.mockInitializedIndex(definitionGeneration), definitionGeneration);
  }

  public static SearchIndexDefinitionGeneration uniqueMockGenerationDefinition() {
    return mockDefinitionGeneration(new ObjectId());
  }

  public static SearchIndexDefinitionGeneration mockDefinitionGeneration(ObjectId indexId) {
    return mockDefinitionGeneration(SearchIndex.mockSearchDefinition(indexId));
  }

  /** Create SearchIndexDefinitionGeneration on an IndexDefinition. */
  public static SearchIndexDefinitionGeneration mockDefinitionGeneration(
      SearchIndexDefinition definition) {
    List<OverriddenBaseAnalyzerDefinition> inferredAnalyzers =
        SearchIndex.mockAnalyzerDefinitions(definition);
    return SearchIndexDefinitionGenerationBuilder.create(
        definition, Generation.CURRENT, inferredAnalyzers);
  }

  public static VectorIndexDefinitionGeneration mockDefinitionGeneration(
      VectorIndexDefinition definition) {
    return new VectorIndexDefinitionGeneration(
        definition, definition.newGeneration(UserIndexVersion.FIRST));
  }

  /** Create SearchIndexDefinitionGeneration for id. */
  public static SearchIndexDefinitionGeneration mockDefinitionGeneration(
      GenerationId generationId) {
    SearchIndexDefinition definition = SearchIndex.mockSearchDefinition(generationId.indexId);
    return SearchIndexDefinitionGenerationBuilder.create(
        definition, generationId.generation, Collections.emptyList());
  }

  static SearchIndexDefinitionGeneration mockDefinitionGeneration(
      SearchIndexDefinition definition, List<OverriddenBaseAnalyzerDefinition> analyzers) {
    return SearchIndexDefinitionGenerationBuilder.create(definition, Generation.CURRENT, analyzers);
  }

  public static VectorIndexDefinitionGeneration mockVectorDefinitionGeneration(ObjectId indexId) {
    return mockDefinitionGeneration(VectorIndex.mockVectorDefinition(indexId));
  }

  public static VectorIndexDefinitionGeneration mockVectorDefinitionGeneration(
      GenerationId generationId) {
    VectorIndexDefinition definition = VectorIndex.mockVectorDefinition(generationId.indexId);
    return new VectorIndexDefinitionGeneration(definition, generationId.generation);
  }

  public static com.xgen.mongot.index.IndexGeneration mockVectorIndexGeneration() {
    return new com.xgen.mongot.index.IndexGeneration(
        VectorIndex.mockIndex(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION),
        VectorIndex.MOCK_INDEX_DEFINITION_GENERATION);
  }

  public static com.xgen.mongot.index.IndexGeneration mockVectorIndexGeneration(
      GenerationId generationId) {
    VectorIndexDefinitionGeneration definition = mockVectorDefinitionGeneration(generationId);
    return new com.xgen.mongot.index.IndexGeneration(
        VectorIndex.mockIndex(definition), VectorIndex.MOCK_INDEX_DEFINITION_GENERATION);
  }
}
