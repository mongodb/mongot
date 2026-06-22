package com.xgen.mongot.config.util;

import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CollectionUtils;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;

public class IndexDefinitions {

  public static Map<ObjectId, List<IndexDefinition>> groupedById(
      List<? extends IndexDefinition> indexDefinitions) {
    return indexDefinitions.stream()
        .collect(Collectors.groupingBy(IndexDefinition::getIndexId, Collectors.toList()));
  }

  /** Index definitions and their generations. */
  public static Map<ObjectId, IndexDefinitionGeneration> versionedById(
      Collection<IndexGeneration> indexes) {
    Check.elementAttributesAreUnique(
        indexes, index -> index.getDefinition().getIndexId(), "indexes", "indexId");

    return indexes.stream()
        .collect(
            CollectionUtils.toMapUnsafe(
                indexGen -> indexGen.getIndex().getDefinition().getIndexId(),
                IndexGeneration::getDefinitionGeneration));
  }

  /** generation ids for all the indexes. */
  public static List<GenerationId> generationIds(List<IndexDefinitionGeneration> definitions) {
    return definitions.stream()
        .map(IndexDefinitionGeneration::getGenerationId)
        .collect(Collectors.toList());
  }

  /** generation ids for all the indexes. */
  public static List<GenerationId> indexesGenerationIds(List<IndexGeneration> indexes) {
    return indexes.stream().map(i -> i.getGenerationId()).collect(Collectors.toList());
  }

  /** index definitions for generations. */
  public static List<IndexDefinition> indexDefinitions(
      List<IndexDefinitionGeneration> definitionGenerations) {
    return definitionGenerations.stream()
        .map(IndexDefinitionGeneration::getIndexDefinition)
        .collect(Collectors.toList());
  }

  public static List<IndexDefinition> allIndexDefinitions(Collection<IndexGeneration> indexes) {
    return indexes.stream()
        .map(IndexGeneration::getIndex)
        .map(Index::getDefinition)
        .collect(Collectors.toList());
  }

  public static List<SearchIndexDefinition> searchIndexDefinitions(
      Collection<IndexGeneration> indexes) {
    return indexes.stream()
        .map(IndexGeneration::getIndex)
        .map(Index::getDefinition)
        .filter(definition -> definition.getType() == IndexDefinition.Type.SEARCH)
        .map(IndexDefinition::asSearchDefinition)
        .collect(Collectors.toList());
  }

  public static List<VectorIndexDefinition> vectorIndexDefinitions(
      Collection<IndexGeneration> indexes) {
    return indexes.stream()
        .map(IndexGeneration::getIndex)
        .map(Index::getDefinition)
        .filter(definition -> definition.getType() == IndexDefinition.Type.VECTOR_SEARCH)
        .map(IndexDefinition::asVectorDefinition)
        .collect(Collectors.toList());
  }

  /** index ids for indexes. */
  public static List<ObjectId> indexIds(List<IndexGeneration> indexes) {
    return indexes.stream()
        .map(IndexGeneration::getIndex)
        .map(Index::getDefinition)
        .map(IndexDefinition::getIndexId)
        .collect(Collectors.toList());
  }
}
