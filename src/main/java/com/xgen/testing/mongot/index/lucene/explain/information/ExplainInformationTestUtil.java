package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.CollectorExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.EqualsWithTimingEquator;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimingBreakdown;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.Equality;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.lucene.explain.timing.ExplainTimingBreakdownBuilder;
import com.xgen.testing.mongot.index.lucene.explain.timing.QueryExecutionAreaBuilder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public class ExplainInformationTestUtil {
  public static final QueryExecutionArea QUERY_EXECUTION_AREA =
      QueryExecutionAreaBuilder.builder().nanosElapsed(10).invocationCounts(Map.of()).build();

  public static final String RESOURCES_PATH =
      "src/test/unit/resources/index/lucene/explain/information/";

  public static final ExplainTimingBreakdown BASIC_STATS =
      ExplainTimingBreakdownBuilder.builder()
          .context(QUERY_EXECUTION_AREA)
          .match(QUERY_EXECUTION_AREA)
          .score(QUERY_EXECUTION_AREA)
          .build();

  /**
   * This {@link Equator} is responsible for testing equality of {@link QueryExplainInformation}.
   * One place it is used is {@link LuceneQuerySpecification#equals(LuceneQuerySpecification,
   * Equator, Comparator)}.
   *
   * <p>We use an Equator to test equality of LuceneExplainInformation because some members of
   * LuceneExplainInformation do not implement {@code equals()} - particularly {@link
   * ExplainTimingBreakdown}, and {@link LuceneQuerySpecification}. This equator does not consider
   * the ExplainTimingBreakdown values when testing equality, and defers equality testing of
   * LuceneQuerySpecification to {@link ExplainInformationTestUtil.QuerySpecificationEquator}.
   */
  public static class QueryExplainInformationEquator implements Equator<QueryExplainInformation> {
    private static final QueryExplainInformationEquator INSTANCE =
        new QueryExplainInformationEquator();

    public static Equator<QueryExplainInformation> equator() {
      return INSTANCE;
    }

    @Override
    public boolean equate(QueryExplainInformation o1, QueryExplainInformation o2) {
      return o1.equals(o2, QuerySpecificationEquator.equator());
    }

    @Override
    public int hash(QueryExplainInformation o) {
      return o.hashCode();
    }
  }

  public static class ListQueryExplainInformationEquator
      implements Equator<List<QueryExplainInformation>> {
    private static final ListQueryExplainInformationEquator INSTANCE =
        new ListQueryExplainInformationEquator();

    public static Equator<List<QueryExplainInformation>> equator() {
      return INSTANCE;
    }

    @Override
    public boolean equate(List<QueryExplainInformation> o1, List<QueryExplainInformation> o2) {
      if (o1.size() != o2.size()) {
        return false;
      }

      var firstCopy = new ArrayList<>(o1);
      firstCopy.sort(totalOrderComparator());
      var secondCopy = new ArrayList<>(o2);
      secondCopy.sort(totalOrderComparator());

      for (int i = 0; i != firstCopy.size(); i++) {
        if (!QueryExplainInformationEquator.equator().equate(firstCopy.get(i), secondCopy.get(i))) {
          return false;
        }
      }

      return true;
    }

    @Override
    public int hash(List<QueryExplainInformation> o) {
      return o.hashCode();
    }
  }

  public static class IgnoreTimingExplainInformationEquator<T extends EqualsWithTimingEquator<T>>
      implements Equator<T> {
    @Override
    public boolean equate(T o1, T o2) {
      if (o1 instanceof CollectorExplainInformation first
          && o2 instanceof CollectorExplainInformation second) {
        // Scoped plans are stored as BSON, but their Boolean/dismax clauses have the same
        // unordered semantics as the top-level query. Compare those with the query equator.
        return withoutScopedPlans(first)
                .equals(withoutScopedPlans(second), Equality.alwaysEqualEquator())
            && scopedPlansEqual(
                first.sortStats().flatMap(SortStats::embeddedDocuments),
                second.sortStats().flatMap(SortStats::embeddedDocuments));
      }
      return o1.equals(o2, Equality.alwaysEqualEquator());
    }

    @Override
    public int hash(T o) {
      return o.hashCode();
    }
  }

  private static CollectorExplainInformation withoutScopedPlans(CollectorExplainInformation info) {
    return new CollectorExplainInformation(
        info.allCollectorStats(),
        info.facetStats(),
        info.sortStats()
            .map(
                stats ->
                    new SortStats(
                        stats.stats(),
                        stats.fieldInfos(),
                        stats.usesIndexSort(),
                        Optional.empty())));
  }

  private static boolean scopedPlansEqual(
      Optional<BsonDocument> first, Optional<BsonDocument> second) {
    if (first.isEmpty() || second.isEmpty()) {
      return first.isEmpty() && second.isEmpty();
    }
    if (!first.get().keySet().equals(second.get().keySet())) {
      return false;
    }
    for (String field : first.get().keySet()) {
      BsonDocument firstScope = first.get().getDocument(field).clone();
      BsonDocument secondScope = second.get().getDocument(field).clone();
      BsonDocument firstQuery = firstScope.getDocument("query");
      BsonDocument secondQuery = secondScope.getDocument("query");
      firstScope.remove("query");
      secondScope.remove("query");
      if (!firstScope.equals(secondScope)
          || !QueryExplainInformationEquator.equator()
              .equate(parseScopedQuery(firstQuery), parseScopedQuery(secondQuery))) {
        return false;
      }
    }
    return true;
  }

  private static QueryExplainInformation parseScopedQuery(BsonDocument query) {
    try (var parser = BsonDocumentParser.fromRoot(query).build()) {
      return QueryExplainInformation.fromBson(parser);
    } catch (BsonParseException e) {
      throw new IllegalArgumentException("Invalid scoped-sort child-query explain", e);
    }
  }

  /**
   * This {@link Equator} is responsible for testing equality of {@link LuceneQuerySpecification}.
   * One place it is used is {@link QueryExplainInformation#equals(QueryExplainInformation,
   * Equator)}.
   *
   * <p>We use an Equator to test equality of LuceneQuerySpecification because some members of
   * LuceneQuerySpecification do not implement {@code equals()} ({@link QueryExplainInformation}).
   * Children of LuceneQuerySpecification may also be unordered, and a comparator to impose total
   * order over LuceneExplainInformation is also useful.
   *
   * <p>This equator defers equality testing of LuceneExplainInformation to {@link
   * QueryExplainInformationEquator}.
   */
  static class QuerySpecificationEquator implements Equator<LuceneQuerySpecification> {
    private static final QuerySpecificationEquator INSTANCE = new QuerySpecificationEquator();

    public static Equator<LuceneQuerySpecification> equator() {
      return INSTANCE;
    }

    @Override
    public boolean equate(LuceneQuerySpecification o1, LuceneQuerySpecification o2) {
      return o1.equals(o2, QueryExplainInformationEquator.equator(), totalOrderComparator());
    }

    @Override
    public int hash(LuceneQuerySpecification o) {
      return o.hashCode();
    }
  }

  /**
   * It is useful to be able to impose a total order on LuceneExplainInformation objects. With a
   * total order, we can directly test otherwise unordered lists of LuceneExplainInformation for
   * equality.
   */
  public static Comparator<QueryExplainInformation> totalOrderComparator() {
    return Comparator.comparing(
        (QueryExplainInformation e) ->
            JsonCodec.toJson(
                // ensure that children of boolean/dismax queries are pre-sorted before converting
                // to json to prevent determinism issues
                e.sortedArgs(
                        Comparator.comparing(
                            (QueryExplainInformation sub) -> JsonCodec.toJson(sub.toBson())))
                    .toBson()));
  }
}
