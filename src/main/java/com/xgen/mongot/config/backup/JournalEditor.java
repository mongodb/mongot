package com.xgen.mongot.config.backup;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Modify a config journal in a fluent API.
 *
 * <p>Works in {@link JournalEntry}s rather than definitions so that an entry which could not be
 * parsed survives an edit.
 */
public class JournalEditor {
  private final List<JournalEntry<? extends IndexDefinitionGeneration>> staged;
  private final List<JournalEntry<? extends IndexDefinitionGeneration>> live;
  private final List<JournalEntry<? extends IndexDefinitionGeneration>> dropped;

  private JournalEditor(
      List<JournalEntry<? extends IndexDefinitionGeneration>> staged,
      List<JournalEntry<? extends IndexDefinitionGeneration>> live,
      List<JournalEntry<? extends IndexDefinitionGeneration>> dropped) {
    this.staged = staged;
    this.live = live;
    this.dropped = dropped;
  }

  /** Create an editor initialized from the state described by the given journal. */
  public static JournalEditor on(ConfigJournalV1 journal) {
    // make mutable copies for entry lists
    return new JournalEditor(
        new ArrayList<>(journal.getStagedEntries()),
        new ArrayList<>(journal.getLiveEntries()),
        new ArrayList<>(journal.getDeletedEntries()));
  }

  /** instantiate a journal from the current edited state. */
  public ConfigJournalV1 journal() {
    return new ConfigJournalV1(this.staged, this.live, this.dropped);
  }

  /** for indexes that reside in the index catalog and should be dropped. */
  public JournalEditor fromLiveToDropped(List<GenerationId> toDrop) {
    return moveEntries(toDrop, this.live, this.dropped, "live");
  }

  /** When staged indexes are swapped into the index catalog. */
  public JournalEditor fromStagedToLive(List<GenerationId> toSwap) {
    return moveEntries(toSwap, this.staged, this.live, "staged");
  }

  /** When staged indexes are dropped. */
  public JournalEditor fromStagedToDropped(List<GenerationId> toDrop) {
    return moveEntries(toDrop, this.staged, this.dropped, "staged");
  }

  private JournalEditor moveEntries(
      List<GenerationId> idsToMove,
      List<JournalEntry<? extends IndexDefinitionGeneration>> source,
      List<JournalEntry<? extends IndexDefinitionGeneration>> dest,
      String sourceName) {
    ImmutableSet<GenerationId> ids = ImmutableSet.copyOf(idsToMove);
    List<JournalEntry<? extends IndexDefinitionGeneration>> entriesToMove =
        source.stream()
            .filter(entry -> ids.contains(entry.getGenerationId()))
            .collect(Collectors.toList());
    checkState(
        entriesToMove.size() == ids.size(),
        "not all indexes to move have been %s: (%s) should be contained in (%s)",
        sourceName,
        idsToMove,
        generationIds(source));

    source.removeAll(entriesToMove);
    dest.addAll(entriesToMove);
    return this;
  }

  private static List<GenerationId> generationIds(
      List<JournalEntry<? extends IndexDefinitionGeneration>> entries) {
    return entries.stream().map(JournalEntry::getGenerationId).collect(Collectors.toList());
  }

  /** Adds these index definitions to live ones. */
  public JournalEditor addLive(List<IndexDefinitionGeneration> added) {
    added.forEach(definition -> this.live.add(JournalEntry.parsed(definition)));
    return this;
  }

  /** Adds a staged index. */
  public JournalEditor addStaged(IndexDefinitionGeneration staged) {
    this.staged.add(JournalEntry.parsed(staged));
    return this;
  }
}
