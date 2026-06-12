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
  /**
   * Builds an internal {@link IndexDefinition} from a user-supplied definition.
   *
   * <p>{@code existingMaterializedViewNameFormatVersion} is the
   * materializedViewNameFormatVersion already stored on a pre-existing index, or
   * {@link Optional#empty()} when none exists. Update callers should pass the value from the
   * prior on-disk definition so the auto-embedding MV name does not shift; create callers pass
   * {@link Optional#empty()} and get the v1 stamp applied. Applies to both vector and search
   * auto-embed indexes, which share the same MV resolver. See CLOUDP-409822.
   */
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
      Instant definitionVersionCreatedAt,
      Optional<Long> existingMaterializedViewNameFormatVersion) {
    var newIndexId = indexId.orElseGet(ObjectId::new);
    return switch (indexDefinition) {
      case UserSearchIndexDefinition searchIndexDefinition ->
          SearchIndexDefinition.create(
              newIndexId,
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
              Optional.of(definitionVersionCreatedAt),
              Optional.of(newIndexId),
              Optional.empty(),
              // Preserve prior value on update; stamp v1 on create
              existingMaterializedViewNameFormatVersion.or(() -> Optional.of(1L)));
      case UserVectorIndexDefinition vectorIndexDefinition ->
          new VectorIndexDefinition(
              newIndexId,
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
              vectorIndexDefinition.nestedRoot(),
              Optional.of(newIndexId),
              Optional.empty(),
              // Preserve prior value on update; stamp v1 on create
              existingMaterializedViewNameFormatVersion.or(() -> Optional.of(1L)));
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

  public static int toIndexPriority(StatusCode statusCode) {
    return ExternalStatus.fromStatusCode(statusCode).getPriority();
  }

  public static String toExternalStatus(StatusCode internal) {
    return ExternalStatus.fromStatusCode(internal).name();
  }

  /**
   * Takes the main index status, the staged index status, and if the main and staged indexes are
   * on the latest index definition to determine the overall status of the index.
   */
  public static StatusCode calculateStatus(
      StatusCode mainIndexStatusCode,
      StatusCode stagedIndexStatusCode,
      boolean isMainIndexLatestVersion,
      boolean isStagedIndexLatestVersion) {

    ExternalStatus mainIndexStatus = ExternalStatus.fromStatusCode(mainIndexStatusCode);
    ExternalStatus stagedIndexStatus = ExternalStatus.fromStatusCode(stagedIndexStatusCode);

    if (mainIndexStatus == ExternalStatus.PENDING || mainIndexStatus == ExternalStatus.BUILDING) {
      return mainIndexStatusCode;
    }

    if (mainIndexStatus == ExternalStatus.DOES_NOT_EXIST) {
      if (stagedIndexStatus == ExternalStatus.READY || stagedIndexStatus == ExternalStatus.STALE) {
        // This should be a temporary state before staged swaps into main but without this special
        // case it would show as READY/STALE but not queryable
        return StatusCode.INITIAL_SYNC;
      } else {
        return stagedIndexStatusCode;
      }
    }

    if (mainIndexStatus == ExternalStatus.READY || mainIndexStatus == ExternalStatus.FAILED) {
      if (isMainIndexLatestVersion) {
        return mainIndexStatusCode;
      }
      if (!isStagedIndexLatestVersion) {
        return StatusCode.INITIAL_SYNC;
      }
      if (stagedIndexStatus == ExternalStatus.FAILED) {
        return StatusCode.FAILED;
      }
      return StatusCode.INITIAL_SYNC;
    }

    if (mainIndexStatus == ExternalStatus.STALE) {
      if (isMainIndexLatestVersion) {
        if (stagedIndexStatus == ExternalStatus.BUILDING
            || stagedIndexStatus == ExternalStatus.READY) {
          return StatusCode.INITIAL_SYNC;
        }
        return mainIndexStatusCode;
      }

      if (stagedIndexStatus == ExternalStatus.FAILED) {
        return mainIndexStatusCode;
      }
      return StatusCode.INITIAL_SYNC;
    }
    throw new IllegalStateException("illegal index status input");
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

  enum ExternalStatus {
    DELETING(6),
    FAILED(5),
    STALE(4),
    PENDING(3),
    BUILDING(2),
    READY(1),
    DOES_NOT_EXIST(0);

    private final int priority;

    private ExternalStatus(int priority) {
      this.priority = priority;
    }

    public int getPriority() {
      return this.priority;
    }

    static ExternalStatus fromStatusCode(StatusCode internal) {
      return switch (internal) {
        case NOT_STARTED, UNKNOWN -> PENDING;
        case INITIAL_SYNC -> BUILDING;
        case STALE, RECOVERING_TRANSIENT, RECOVERING_NON_TRANSIENT -> STALE;
        case STEADY -> READY;
        case FAILED -> FAILED;
        case DOES_NOT_EXIST -> DOES_NOT_EXIST;
      };
    }
  }
}
