package com.xgen.testing.mongot.config.backup;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEntry;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigJournalV1Builder {

  private final List<IndexDefinitionGeneration> stagedIndexes = new ArrayList<>();
  private final List<IndexDefinitionGeneration> indexes = new ArrayList<>();
  private final List<IndexDefinitionGeneration> deletedIndexes = new ArrayList<>();

  public static ConfigJournalV1Builder builder() {
    return new ConfigJournalV1Builder();
  }

  public ConfigJournalV1Builder stagedIndex(IndexDefinitionGeneration index) {
    this.stagedIndexes.add(index);
    return this;
  }

  public ConfigJournalV1Builder liveIndex(IndexDefinitionGeneration index) {
    this.indexes.add(index);
    return this;
  }

  public ConfigJournalV1Builder deletedIndex(IndexDefinitionGeneration index) {
    this.deletedIndexes.add(index);
    return this;
  }

  public ConfigJournalV1 build() {
    return new ConfigJournalV1(
        asEntries(this.stagedIndexes), asEntries(this.indexes), asEntries(this.deletedIndexes));
  }

  private static List<JournalEntry<? extends IndexDefinitionGeneration>> asEntries(
      List<IndexDefinitionGeneration> definitions) {
    return definitions.stream()
        .map(JournalEntry::<IndexDefinitionGeneration>parsed)
        .collect(Collectors.toList());
  }
}
