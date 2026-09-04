package com.xgen.testing.mongot.config.backup;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEntry;
import com.xgen.mongot.config.manager.QuarantinedIndex;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import java.util.ArrayList;
import java.util.List;

public class ConfigJournalV1Builder {

  private final List<JournalEntry<? extends IndexDefinitionGeneration>> stagedIndexes =
      new ArrayList<>();
  private final List<JournalEntry<? extends IndexDefinitionGeneration>> indexes = new ArrayList<>();
  private final List<JournalEntry<? extends IndexDefinitionGeneration>> deletedIndexes =
      new ArrayList<>();

  public static ConfigJournalV1Builder builder() {
    return new ConfigJournalV1Builder();
  }

  public ConfigJournalV1Builder stagedIndex(IndexDefinitionGeneration index) {
    return stagedEntry(JournalEntry.parsed(index));
  }

  public ConfigJournalV1Builder liveIndex(IndexDefinitionGeneration index) {
    return liveEntry(JournalEntry.parsed(index));
  }

  public ConfigJournalV1Builder deletedIndex(IndexDefinitionGeneration index) {
    return deletedEntry(JournalEntry.parsed(index));
  }

  /** Adds a staged entry as-is, so that a quarantined entry can be journaled. */
  public ConfigJournalV1Builder stagedEntry(
      JournalEntry<? extends IndexDefinitionGeneration> entry) {
    this.stagedIndexes.add(entry);
    return this;
  }

  /** Adds a live entry as-is, so that a quarantined entry can be journaled. */
  public ConfigJournalV1Builder liveEntry(JournalEntry<? extends IndexDefinitionGeneration> entry) {
    this.indexes.add(entry);
    return this;
  }

  /** Adds a deleted entry as-is, so that a quarantined entry can be journaled. */
  public ConfigJournalV1Builder deletedEntry(
      JournalEntry<? extends IndexDefinitionGeneration> entry) {
    this.deletedIndexes.add(entry);
    return this;
  }

  /** Adds a staged entry whose definition could not be parsed. */
  public ConfigJournalV1Builder quarantinedStagedIndex(QuarantinedIndex index) {
    return stagedEntry(index.asJournalEntry());
  }

  /** Adds a live entry whose definition could not be parsed. */
  public ConfigJournalV1Builder quarantinedLiveIndex(QuarantinedIndex index) {
    return liveEntry(index.asJournalEntry());
  }

  /** Adds a deleted entry whose definition could not be parsed. */
  public ConfigJournalV1Builder quarantinedDeletedIndex(QuarantinedIndex index) {
    return deletedEntry(index.asJournalEntry());
  }

  public ConfigJournalV1 build() {
    return new ConfigJournalV1(this.stagedIndexes, this.indexes, this.deletedIndexes);
  }
}
