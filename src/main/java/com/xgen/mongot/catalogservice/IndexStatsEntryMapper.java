package com.xgen.mongot.catalogservice;

import com.xgen.mongot.catalogservice.schema.IndexStatsEntry;
import com.xgen.mongot.index.IndexDetailedStatus;
import com.xgen.mongot.index.IndexInformation;
import java.util.Optional;
import org.bson.types.ObjectId;

/** Utility class to map {@link IndexInformation} into {@link IndexStatsEntry} objects. */
public class IndexStatsEntryMapper {

  public static IndexStatsEntry fromIndexInformation(
      IndexInformation indexInfo, ObjectId serverId) {
    return new IndexStatsEntry(
        new IndexStatsEntry.IndexStatsKey(serverId, indexInfo.getDefinition().getIndexId()),
        indexInfo.getDefinition().getType(),
        indexInfo.getMainIndex().map(IndexStatsEntryMapper::fromIndexDetailedStatus),
        indexInfo.getStagedIndex().map(IndexStatsEntryMapper::fromIndexDetailedStatus));
  }

  private static IndexStatsEntry.DetailedIndexStats fromIndexDetailedStatus(
      IndexDetailedStatus indexDetailedStatus) {
    return switch (indexDetailedStatus) {
      case IndexDetailedStatus.Search search -> fromSearchIndexDetailedStatus(search);
      case IndexDetailedStatus.Vector vector -> fromVectorIndexDetailedStatus(vector);
    };
  }

  private static IndexStatsEntry.DetailedIndexStats fromSearchIndexDetailedStatus(
      IndexDetailedStatus.Search indexDetailedStatus) {
    return new IndexStatsEntry.DetailedIndexStats(
        indexDetailedStatus.indexStatus(),
        indexDetailedStatus.definition(),
        Optional.of(indexDetailedStatus.synonymStatusMap()));
  }

  private static IndexStatsEntry.DetailedIndexStats fromVectorIndexDetailedStatus(
      IndexDetailedStatus.Vector indexDetailedStatus) {
    return new IndexStatsEntry.DetailedIndexStats(
        indexDetailedStatus.indexStatus(), indexDetailedStatus.definition(), Optional.empty());
  }
}
