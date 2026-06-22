package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.util.IndexDefinitions;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.index.IndexGeneration;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PhasingOutIndexesDropper {
  private static final Logger LOG = LoggerFactory.getLogger(PhasingOutIndexesDropper.class);

  /** Drop indexes that are phasing out if they are not in use anymore. */
  static void dropUnused(ConfigState configState, IndexActions indexActions)
      throws IOException, Invariants.InvariantException {
    List<IndexGeneration> readyToDrop =
        configState.phasingOut.getIndexes().stream()
            .filter(indexGen -> noOpenCursors(configState, indexGen))
            .collect(Collectors.toList());
    if (readyToDrop.isEmpty()) {
      return;
    }

    IndexDefinitions.indexesGenerationIds(readyToDrop)
        .forEach(
            e ->
                LOG.atWarn()
                    .addKeyValue("indexId", e.indexId)
                    .addKeyValue("generationId", e.generation)
                    .log("dropping phasing out index that is no longer used"));

    // the indexes in configState.phasingOut should already be journaled as dropped indexes, we can
    // go ahead and drop them.
    indexActions.dropFromPhasingOut(readyToDrop);

    // journal our current state without the dropped indexes.
    configState.persist(configState.currentJournal());
  }

  private static boolean noOpenCursors(ConfigState configState, IndexGeneration index) {
    return !configState.cursorManager.hasOpenCursors(index.getGenerationId());
  }
}
