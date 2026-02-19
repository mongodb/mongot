package com.xgen.mongot.index.lucene.explain.knn;

import com.google.common.base.Supplier;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchSegmentStatsSpec;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchTracingSpec;
import com.xgen.mongot.index.lucene.explain.knn.VectorSearchExplainer.TracingInformation;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.Range;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;

/** Helper class to consolidate logic for instrumentation of KNN queries */
public class KnnInstrumentationHelper {
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final VectorSearchExplainer tracingExplainer;
  private final String fieldName;
  private final int limit;
  private final boolean filterPresent;

  public KnnInstrumentationHelper(
      VectorSearchExplainer tracingExplainer, String fieldName, int limit, boolean filterPresent) {
    this.tracingExplainer = tracingExplainer;
    this.fieldName = fieldName;
    this.limit = limit;
    this.filterPresent = filterPresent;
  }

  /** Wraps 'approximateSearch' method invocation and records related stats. */
  public TopDocs meteredApproximateSearch(
      LeafReaderContext context,
      @Nullable Bits acceptDocs,
      CheckedSupplier<TopDocs, IOException> approximateSearch)
      throws IOException {

    VectorSearchExplainer.SegmentStatistics segmentStatistics =
        getBaseSegmentStats(
            context, () -> Optional.ofNullable(acceptDocs).map(this::cardinalityOfBits));

    List<TracingInformation> infos = tracingInfosForSegment(context);
    for (TracingInformation info : infos) {
      info.setExecutionType(VectorSearchSegmentStatsSpec.SegmentExecutionType.APPROXIMATE);
    }

    if (this.filterPresent && acceptDocs != null) {
      checkFilteredOutDocuments(infos, acceptDocs, context);
    }

    try (var unused =
        segmentStatistics.getTimings().split(ExplainTimings.Type.VECTOR_SEARCH_APPROXIMATE)) {
      return approximateSearch.get();
    }
  }

  /** Wraps 'exactSearch' method invocation and records related stats. */
  public TopDocs meteredExactSearch(
      LeafReaderContext context,
      DocIdSetIterator acceptIterator,
      CheckedSupplier<TopDocs, IOException> exactSearch)
      throws IOException {

    VectorSearchExplainer.SegmentStatistics segmentStats =
        getBaseSegmentStats(context, () -> Optional.of((int) acceptIterator.cost()));

    List<TracingInformation> infos = tracingInfosForSegment(context);
    for (TracingInformation info : infos) {
      if (info.getExecutionType().isPresent()
          && info.getExecutionType().get()
              == VectorSearchSegmentStatsSpec.SegmentExecutionType.APPROXIMATE) {
        info.setExecutionType(
            VectorSearchSegmentStatsSpec.SegmentExecutionType.APPROXIMATE_FALLBACK_TO_EXACT);
      } else {
        info.setExecutionType(VectorSearchSegmentStatsSpec.SegmentExecutionType.EXACT);
      }
    }

    if (this.filterPresent) {
      if (acceptIterator instanceof BitSetIterator bitSetIterator) {
        // it always has to be BitSetIterator
        checkFilteredOutDocuments(infos, bitSetIterator.getBitSet(), context);
      } else {
        FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
            "acceptIterator is not a BitSetIterator");
      }
    }

    try (var unused = segmentStats.getTimings().split(ExplainTimings.Type.VECTOR_SEARCH_EXACT)) {
      return exactSearch.get();
    }
  }

  private static void checkFilteredOutDocuments(
      List<TracingInformation> infos, Bits acceptDocs, LeafReaderContext context) {
    for (var tracingInfo : infos) {
      int localLuceneId = globalLuceneIdToLocal(tracingInfo.getTarget().luceneDocId(), context);
      if (!acceptDocs.get(localLuceneId)) {
        tracingInfo.setDropReason(VectorSearchTracingSpec.DropReason.FILTER);
      }
    }
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public TopDocs meteredMergeLeafResults(
      TopDocs[] perLeafResults, Supplier<TopDocs> mergeLeafResults) {

    // Once collection has been completed in all segments, we can observe the
    // merge of per-segment results to see if the traced doc was dropped there.
    List<TracingInformation> checkForMergeDrop = checkForNonCompetitiveScore(perLeafResults);

    TopDocs mergeResults = mergeLeafResults.get();
    if (checkForMergeDrop.isEmpty()) {
      return mergeResults;
    }

    Map<Integer, Integer> resultDocsIdsToRank = new HashMap<>();
    for (int i = 0; i < mergeResults.scoreDocs.length; i++) {
      resultDocsIdsToRank.put(mergeResults.scoreDocs[i].doc, i);
    }

    for (TracingInformation tracingInfo : checkForMergeDrop) {
      if (!resultDocsIdsToRank.containsKey(tracingInfo.getTarget().luceneDocId())) {
        tracingInfo.setDropReason(VectorSearchTracingSpec.DropReason.MERGE);
        continue;
      }

      int rank = resultDocsIdsToRank.get(tracingInfo.getTarget().luceneDocId());
      if (rank >= this.limit) {
        // this doc fits into "numCandidates", but will be filtered out by "limit" cap.
        tracingInfo.setDropReason(VectorSearchTracingSpec.DropReason.LIMIT_CAP);
      }
    }
    return mergeResults;
  }

  private List<TracingInformation> checkForNonCompetitiveScore(TopDocs[] perLeafResults) {
    if (this.tracingExplainer.getTracingInformationWithVectors().isEmpty()) {
      return List.of();
    }

    List<TracingInformation> competitiveTracingDocs = new ArrayList<>();
    Set<Integer> allResultsBeforeMerge =
        Arrays.stream(perLeafResults)
            .flatMap(result -> Arrays.stream(result.scoreDocs))
            .map(doc -> doc.doc)
            .collect(Collectors.toSet());

    for (var tracingInfo : this.tracingExplainer.getTracingInformationWithVectors()) {
      if (noNeedToCheckForDropReason(tracingInfo)) {
        continue;
      }

      if (allResultsBeforeMerge.contains(tracingInfo.getTarget().luceneDocId())) {
        competitiveTracingDocs.add(tracingInfo);
      } else {
        tracingInfo.setDropReason(VectorSearchTracingSpec.DropReason.NON_COMPETITIVE_SCORE);
      }
    }
    return competitiveTracingDocs;
  }

  public void examineResultsAfterRescoring(TopDocs rescoredDocs) {
    Set<Integer> luceneDocIdsAfterRescoring =
        Arrays.stream(rescoredDocs.scoreDocs).map((doc) -> doc.doc).collect(Collectors.toSet());

    for (TracingInformation tracingInfo :
        this.tracingExplainer.getTracingInformationWithVectors()) {
      if (noNeedToCheckForDropReason(tracingInfo)) {
        continue;
      }

      int luceneDocId = tracingInfo.getTarget().luceneDocId();
      if (!luceneDocIdsAfterRescoring.contains(luceneDocId)) {
        tracingInfo.setDropReason(VectorSearchTracingSpec.DropReason.RESCORING);
      }
    }
  }

  private static boolean noNeedToCheckForDropReason(TracingInformation tracingInfo) {
    if (tracingInfo.getDropReason().isPresent()) {
      return true;
    }

    VectorSearchSegmentStatsSpec.SegmentExecutionType executionType =
        Check.isPresent(tracingInfo.getExecutionType(), "executionType");
    return !tracingInfo.isVisited()
        && executionType != VectorSearchSegmentStatsSpec.SegmentExecutionType.EXACT;
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public AbstractKnnCollector getKnnCollector(LeafReaderContext context, int k, int visitedLimit)
      throws IOException {

    AbstractKnnCollector knnCollector = createKnnCollector(context, k, visitedLimit);
    this.tracingExplainer.getSegmentStats(context.id()).replaceCollectorAndMergeStats(knnCollector);
    return knnCollector;
  }

  private AbstractKnnCollector createKnnCollector(
      LeafReaderContext context, int k, int visitedLimit) throws IOException {
    List<TracingInformation> tracingDocsInSegment = tracingInfosForSegment(context);
    if (tracingDocsInSegment.isEmpty()) {
      return new TopKnnCollector(k, visitedLimit);
    }

    Map<Integer, TracingInformation> luceneIdToTracingInfo =
        new HashMap<>(tracingDocsInSegment.size());
    for (TracingInformation tracingInfo : tracingDocsInSegment) {
      luceneIdToTracingInfo.put(tracingInfo.getTarget().luceneDocId(), tracingInfo);
    }

    // Checking which nodes are unreachable.
    Set<Integer> unreachableNodes =
        ReachabilityChecker.identifyUnreachable(
            context, this.fieldName, luceneIdToTracingInfo.keySet());
    for (int node : unreachableNodes) {
      // 1. Removing it from "to check" list, so that we could shortcut in case all tracing
      // nodes are unreachable
      // 2. Marking node as unreachable
      luceneIdToTracingInfo.remove(node).markAsUnreachable();
    }

    if (luceneIdToTracingInfo.isEmpty()) {
      return new TopKnnCollector(k, visitedLimit);
    }

    return new InstrumentedTopKnnCollector(context.docBase, this, k, visitedLimit);
  }

  public void examineCollect(int docId, float score) {
    Optional<TracingInformation> tracingInfoOpt =
        this.tracingExplainer.getTracingInformation(docId);

    if (tracingInfoOpt.isEmpty()) {
      return;
    }

    TracingInformation tracingInfo = tracingInfoOpt.get();
    tracingInfo.setVisited(true);
    tracingInfo.setScore(score);
  }

  private VectorSearchExplainer.SegmentStatistics getBaseSegmentStats(
      LeafReaderContext context, Supplier<Optional<Integer>> matchingDocsCount) {
    int docCount = context.reader().numDocs();

    VectorSearchExplainer.SegmentStatistics segmentStats =
        this.tracingExplainer
            .getSegmentStats(context.id())
            .setDocCount(docCount)
            .setSegmentId(resolveSegmentId(context));

    if (this.filterPresent) {
      // The 'orElseGet' case should never occur, because if a filter is present, matchingDocsCount
      // will never be empty. If it does occur, set it to docCount to indicate that all documents
      // matched.
      int filterMatchedDocsCount =
          matchingDocsCount
              .get()
              .orElseGet(
                  () -> {
                    FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
                        "filterMatchedDocsCount is empty when the filter is present");
                    return docCount;
                  });
      return segmentStats.setFilterMatchedDocsCount(filterMatchedDocsCount);
    }

    return segmentStats;
  }

  private List<TracingInformation> tracingInfosForSegment(LeafReaderContext context) {
    Optional<String> segmentId = resolveSegmentId(context);
    return this.tracingExplainer.getTracingInformationWithVectors().stream()
        .filter(info -> docIsInSegment(info.getTarget().luceneDocId(), context))
        .peek(info -> info.setSegmentId(segmentId))
        .toList();
  }

  private static Optional<String> resolveSegmentId(LeafReaderContext context) {
    if (context.reader() instanceof SegmentReader segmentReader) {
      return Optional.of(segmentReader.getSegmentName());
    }

    FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log("Could not retrieve segment id");
    return Optional.empty();
  }

  private static boolean docIsInSegment(int docId, LeafReaderContext context) {
    int segmentStart = context.docBase;
    int segmentEnd = context.docBase + context.reader().maxDoc();

    return Range.of(segmentStart, segmentEnd - 1).contains(docId);
  }

  private static int globalLuceneIdToLocal(int globalLuceneDocId, LeafReaderContext context) {
    return globalLuceneDocId - context.docBase;
  }

  private int cardinalityOfBits(Bits bits) {
    if (bits instanceof BitSet bitSet) {
      return bitSet.cardinality();
    }

    // go slow route counting each bit one by one
    @Var int cardinality = 0;
    for (int i = 0; i < bits.length(); i++) {
      if (bits.get(i)) {
        cardinality++;
      }
    }
    return cardinality;
  }
}
