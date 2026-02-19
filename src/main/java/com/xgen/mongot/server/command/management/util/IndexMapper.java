package com.xgen.mongot.server.command.management.util;

import static com.xgen.mongot.index.definition.IndexDefinition.Type;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.server.command.management.definition.common.UserIndexDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserSearchIndexDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserVectorIndexDefinition;
import com.xgen.mongot.util.Enums;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import org.bson.types.ObjectId;

public class IndexMapper {
  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static IndexDefinition toInternal(
      String indexName,
      Optional<ObjectId> indexId,
      UserIndexDefinition indexDefinition,
      UUID collectionUuid,
      String db,
      String collectionName,
      Optional<ViewDefinition> view,
      Long definitionVersion,
      Instant definitionVersionCreatedAt) {
    return switch (indexDefinition) {
      case UserSearchIndexDefinition searchIndexDefinition ->
          SearchIndexDefinition.create(
              indexId.orElseGet(ObjectId::new),
              indexName,
              db,
              collectionName,
              collectionUuid,
              view,
              searchIndexDefinition.numPartitions(),
              searchIndexDefinition.mappings(),
              searchIndexDefinition.analyzer(),
              searchIndexDefinition.searchAnalyzer(),
              searchIndexDefinition.analyzers(),
              SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
              searchIndexDefinition.synonyms(),
              searchIndexDefinition.storedSource(),
              searchIndexDefinition.typeSets(),
              searchIndexDefinition.sort(),
              Optional.of(definitionVersion),
              Optional.of(definitionVersionCreatedAt));
      case UserVectorIndexDefinition vectorIndexDefinition ->
          new VectorIndexDefinition(
              indexId.orElseGet(ObjectId::new),
              indexName,
              db,
              collectionName,
              collectionUuid,
              view,
              vectorIndexDefinition.numPartitions(),
              vectorIndexDefinition.fields(),
              VectorIndexCapabilities.CURRENT_FEATURE_VERSION,
              Optional.of(definitionVersion),
              Optional.of(definitionVersionCreatedAt),
              vectorIndexDefinition.storedSource(),
              vectorIndexDefinition.nestedRoot());
    };
  }

  public static UserIndexDefinition toExternal(IndexDefinition index) {
    return switch (index) {
      case SearchIndexDefinition search ->
          new UserSearchIndexDefinition(
              search.getAnalyzerName(),
              search.getSearchAnalyzerName(),
              search.getMappings(),
              omitDefault(search.getAnalyzers(), Collections.emptyList()),
              omitDefault(search.getStoredSource(), StoredSourceDefinition.defaultValue()),
              search.getTypeSets(),
              search.getSort(),
              search.getSynonyms(),
              search.getNumPartitions());
      case VectorIndexDefinition vector ->
          new UserVectorIndexDefinition(
              vector.getFields(),
              vector.getNumPartitions(),
              omitDefault(vector.getStoredSource(), StoredSourceDefinition.defaultValue()),
              vector.getNestedRoot());
    };
  }

  public static String toExternalStatus(StatusCode internal) {
    return switch (internal) {
      case NOT_STARTED, UNKNOWN -> "PENDING";
      case INITIAL_SYNC -> "BUILDING";
      case STALE, RECOVERING_TRANSIENT, RECOVERING_NON_TRANSIENT -> "STALE";
      case STEADY -> "READY";
      case FAILED -> "FAILED";
      case DOES_NOT_EXIST -> "DOES_NOT_EXIST";
    };
  }

  public static String toExternalSynonymStatus(SynonymStatus.External externalStatus) {
    return switch (externalStatus) {
      case BUILDING -> "BUILDING";
      case READY -> "READY";
      case FAILED -> "FAILED";
    };
  }

  public static String toExternalType(IndexDefinition.Type type) {
    return Enums.convertNameTo(CaseFormat.LOWER_CAMEL, type);
  }

  private static <T> Optional<T> omitDefault(T actual, T defaultValue) {
    if (actual.equals(defaultValue)) {
      return Optional.empty();
    } else {
      return Optional.of(actual);
    }
  }

  /** Two indexes definitions are equivalent if all properties except ID are equal. */
  public static boolean areEquivalent(IndexDefinition idx1, IndexDefinition idx2) {
    if (idx1.getType() == Type.SEARCH && idx2.getType() == Type.SEARCH) {
      return Stream.<Function<SearchIndexDefinition, Object>>of(
              SearchIndexDefinition::getAnalyzerName,
              SearchIndexDefinition::getAnalyzers,
              SearchIndexDefinition::getMappings,
              SearchIndexDefinition::getSearchAnalyzerName,
              SearchIndexDefinition::getStoredSource,
              SearchIndexDefinition::getTypeSets,
              SearchIndexDefinition::getSynonyms,
              SearchIndexDefinition::getNumPartitions)
          .allMatch(
              getter ->
                  getter
                      .apply(idx1.asSearchDefinition())
                      .equals(getter.apply(idx2.asSearchDefinition())));
    } else if (idx1.getType() == Type.VECTOR_SEARCH && idx2.getType() == Type.VECTOR_SEARCH) {
      return Stream.<Function<VectorIndexDefinition, Object>>of(
              VectorIndexDefinition::getFields, VectorIndexDefinition::getNumPartitions)
          .allMatch(
              getter ->
                  getter
                      .apply(idx1.asVectorDefinition())
                      .equals(getter.apply(idx2.asVectorDefinition())));
    } else {
      return false;
    }
  }
}
