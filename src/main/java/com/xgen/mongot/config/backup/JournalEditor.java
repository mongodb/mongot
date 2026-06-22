package com.xgen.mongot.config.backup;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.config.util.IndexDefinitions;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Modify a config journal in a fluent API. */
public class JournalEditor {
  private final List<IndexDefinitionGeneration> staged;
  private final List<IndexDefinitionGeneration> live;
  private final List<IndexDefinitionGeneration> dropped;

  private JournalEditor(
      ArrayList<IndexDefinitionGeneration> staged,
      ArrayList<IndexDefinitionGeneration> live,
      ArrayList<IndexDefinitionGeneration> dropped) {
    this.staged = staged;
    this.live = live;
    this.dropped = dropped;
  }

  /** Create an editor initialized from the state described by the given journal. */
  public static JournalEditor on(ConfigJournalV1 journal) {
    // make mutable copies for definition lists
    return new JournalEditor(
        new ArrayList<>(journal.getStagedIndexes()),
        new ArrayList<>(journal.getLiveIndexes()),
        new ArrayList<>(journal.getDeletedIndexes()));
  }

  /** instantiate a journal from the current edited state. */
  public ConfigJournalV1 journal() {
    return new ConfigJournalV1(
        ImmutableList.copyOf(this.staged),
        ImmutableList.copyOf(this.live),
        ImmutableList.copyOf(this.dropped));
  }

  /** for indexes that reside in the index catalog and should be dropped. */
  public JournalEditor fromLiveToDropped(List<GenerationId> toDrop) {
    return moveDefinitions(toDrop, this.live, this.dropped, "live");
  }

  /** When staged indexes are swapped into the index catalog. */
  public JournalEditor fromStagedToLive(List<GenerationId> toSwap) {
    return moveDefinitions(toSwap, this.staged, this.live, "staged");
  }

  /** When staged indexes are dropped. */
  public JournalEditor fromStagedToDropped(List<GenerationId> toDrop) {
    return moveDefinitions(toDrop, this.staged, this.dropped, "staged");
  }

  private JournalEditor moveDefinitions(
      List<GenerationId> idsToMove,
      List<IndexDefinitionGeneration> source,
      List<IndexDefinitionGeneration> dest,
      String sourceName) {
    ImmutableSet<GenerationId> ids = ImmutableSet.copyOf(idsToMove);
    List<IndexDefinitionGeneration> definitionsToMove =
        source.stream()
            .filter(def -> ids.contains(def.getGenerationId()))
            .collect(Collectors.toList());
    checkState(
        definitionsToMove.size() == ids.size(),
        "not all indexes to move have been %s: (%s) should be contained in (%s)",
        sourceName,
        idsToMove,
        IndexDefinitions.generationIds(source));

    source.removeAll(definitionsToMove);
    dest.addAll(definitionsToMove);
    return this;
  }

  /** Adds these index definitions to live ones. */
  public JournalEditor addLive(List<IndexDefinitionGeneration> added) {
    this.live.addAll(added);
    return this;
  }

  /** Adds a staged index. */
  public JournalEditor addStaged(IndexDefinitionGeneration staged) {
    this.staged.add(staged);
    return this;
  }
}
