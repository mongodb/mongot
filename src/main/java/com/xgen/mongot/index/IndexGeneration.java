package com.xgen.mongot.index;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.IndexTypeData.IndexTypeTag;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;

/** Couples an Index with its generation information. */
public class IndexGeneration {

  private final Index index;
  private final IndexDefinitionGeneration definitionGeneration;

  public IndexGeneration(Index index, IndexDefinitionGeneration definitionGeneration) {
    checkArg(
        index.isCompatibleWith(definitionGeneration.getIndexDefinition()),
        "Index is incompatible with the definition");
    this.index = index;
    this.definitionGeneration = definitionGeneration;
  }

  public IndexDefinitionGeneration getDefinitionGeneration() {
    return this.definitionGeneration;
  }

  /** Syntax sugar to get the GenerationId. */
  public GenerationId getGenerationId() {
    return this.definitionGeneration.getGenerationId();
  }

  public IndexDefinition getDefinition() {
    return this.definitionGeneration.getIndexDefinition();
  }

  /** The overridden analyzers bound to this generation, or empty when it has none. */
  public ImmutableList<OverriddenBaseAnalyzerDefinition> getOverriddenAnalyzers() {
    return this.definitionGeneration instanceof SearchIndexDefinitionGeneration search
        ? search.definition().analyzerDefinitions()
        : ImmutableList.of();
  }

  public Index getIndex() {
    return this.index;
  }

  public IndexDefinitionGeneration.Type getType() {
    return this.definitionGeneration.getType();
  }

  /**
   * Returns the {@code indexType} tag for metrics on this index. Defaults to the tag implied by
   * {@link #getDefinition()}; auto-embedding derived Lucene indexes override this with the tag from
   * the original user definition.
   */
  public IndexTypeTag getMetricsIndexTypeTag() {
    return IndexTypeData.getIndexTypeTag(getDefinition());
  }
}
