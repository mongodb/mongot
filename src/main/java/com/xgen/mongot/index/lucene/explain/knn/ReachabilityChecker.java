package com.xgen.mongot.index.lucene.explain.knn;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.HnswGraph;

/**
 * Utility class that checks whether nodes in the HNSW graph are reachable. Due to its randomized
 * nature, HNSW can form disconnected components, which may leave certain nodes completely
 * unreachable.
 */
public class ReachabilityChecker {
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  /**
   * Checks whether the provided Lucene docIds are completely unreachable from the root of the HNSW
   * graph.
   *
   * @param context the Lucene leaf context
   * @param fieldName the name of the field containing the HNSW graph
   * @param luceneDocIdsToCheck the set of Lucene doc IDs to check
   * @return the set of unreachable nodes (subset of the input)
   * @throws IOException if an I/O error occurs while reading the graph
   * @throws IllegalStateException if the vector reader for the provided field is not an
   *     HnswGraphProvider
   */
  public static Set<Integer> identifyUnreachable(
      LeafReaderContext context, String fieldName, Set<Integer> luceneDocIdsToCheck)
      throws IOException {

    Optional<HnswGraph> hnswGraphOpt = retrieveHnswGraph(context.reader(), fieldName);
    if (hnswGraphOpt.isEmpty()) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "Skipping reachability check for field %s; no HNSW graph found.", fieldName);
      return luceneDocIdsToCheck;
    }

    HnswGraph graph = hnswGraphOpt.get();
    int maxDoc = context.reader().maxDoc();

    // Lucene 10.4 introduced a tinySegmentsThreshold: segments below it skip the HNSW graph and
    // fall back to exact scan, so an empty graph reports numLevels=0. Treat every doc that's
    // actually in the segment as reachable; only flag IDs that fall outside the segment range.
    if (graph.numLevels() == 0) {
      return luceneDocIdsToCheck.stream()
          .filter(id -> id < context.docBase || id >= context.docBase + maxDoc)
          .collect(Collectors.toSet());
    }

    // convert docIds to leaf ordinals
    Set<Integer> leafOrdinalsToCheck =
        luceneDocIdsToCheck.stream().map(id -> id - context.docBase).collect(Collectors.toSet());

    Set<Integer> unreachableNodes = checkForUnreachableNodes(graph, leafOrdinalsToCheck);

    // convert back to docIds
    return unreachableNodes.stream().map(ord -> ord + context.docBase).collect(Collectors.toSet());
  }

  private static Optional<HnswGraph> retrieveHnswGraph(LeafReader leafReader, String fieldName)
      throws IOException {

    if (!(leafReader instanceof CodecReader codecReader)) {
      return Optional.empty();
    }

    if (!(codecReader.getVectorReader()
        instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader)) {
      return Optional.empty();
    }

    KnnVectorsReader vectorsReader = fieldsReader.getFieldReader(fieldName);
    if (!(vectorsReader instanceof HnswGraphProvider hnswVectorsReader)) {
      return Optional.empty();
    }

    return Optional.of(hnswVectorsReader.getGraph(fieldName));
  }

  @VisibleForTesting
  static Set<Integer> checkForUnreachableNodes(HnswGraph graph, Set<Integer> luceneDocIdsToCheck)
      throws IOException {

    Set<Integer> unseenDocIds = new HashSet<>(luceneDocIdsToCheck);
    int numLevels = graph.numLevels();

    Deque<Integer> toVisit = new ArrayDeque<>();
    int entryNode = graph.entryNode();
    // shouldn't be possible, just a precaution.
    if (entryNode < 0) {
      return luceneDocIdsToCheck;
    }
    toVisit.push(entryNode);

    FixedBitSet connectedNodes = new FixedBitSet(graph.maxNodeId() + 1);
    for (int level = numLevels - 1; level >= 0; level--) {
      while (!toVisit.isEmpty()) {
        int node = toVisit.pop();
        if (connectedNodes.get(node)) {
          continue;
        }
        connectedNodes.set(node);

        // remove from unseen if it is present
        unseenDocIds.remove(node);

        // shortcut if we found everything we wanted
        if (unseenDocIds.isEmpty()) {
          return unseenDocIds;
        }

        graph.seek(level, node);
        @Var int friendOrd;
        while ((friendOrd = graph.nextNeighbor()) != NO_MORE_DOCS) {
          if (connectedNodes.get(friendOrd)) {
            continue;
          }
          toVisit.push(friendOrd);
        }
      }

      if (level > 0) {
        // all connected nodes from upper level become new BFS roots.
        // also, we clear the bitset for reuse in the next level
        @Var int ord;
        DocIdSetIterator iter = new BitSetIterator(connectedNodes, connectedNodes.cardinality());
        while ((ord = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          toVisit.push(ord);
          // cleaning it up along the way
          connectedNodes.flip(ord);
        }
      }
    }
    return unseenDocIds;
  }
}
