package com.xgen.mongot.index.query.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.query.optimizer.DifferentialTestHarness.DocField;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

/**
 * Self-test for {@link DifferentialTestHarness}. Drives the equivalence check directly with
 * synthetically-constructed Lucene queries — one pair that is structurally different but matches
 * the same docs (equivalent), and one pair whose match sets are disjoint (inequivalent).
 */
public class DifferentialTestHarnessTest {

  @Test
  public void equivalent_pair_isReportedEquivalent() throws Exception {
    try (var harness =
        DifferentialTestHarness.builder()
            .addDocument("doc-a", DocField.of("category", "fiction"))
            .addDocument("doc-b", DocField.of("category", "fiction"))
            .addDocument("doc-c", DocField.of("category", "non-fiction"))
            .build()) {

      // Trivially-equivalent shapes: bare TermQuery vs single-clause BooleanQuery wrapping it.
      var bare = new TermQuery(new Term("category", "fiction"));
      var wrapped = new BooleanQuery.Builder().add(bare, BooleanClause.Occur.MUST).build();

      var result = harness.compare(bare, wrapped);

      assertTrue(result.describe(), result.equivalent());
      assertEquals(Set.of("doc-a", "doc-b"), result.matchedByOriginal());
      assertEquals(result.matchedByOriginal(), result.matchedByRewritten());
      assertTrue(result.onlyInOriginal().isEmpty());
      assertTrue(result.onlyInRewritten().isEmpty());
    }
  }

  @Test
  public void inequivalent_pair_identifiesDifferingDocs() throws Exception {
    try (var harness =
        DifferentialTestHarness.builder()
            .addDocument("doc-a", DocField.of("category", "fiction"))
            .addDocument("doc-b", DocField.of("category", "fiction"))
            .addDocument("doc-c", DocField.of("category", "non-fiction"))
            .build()) {

      var original = new TermQuery(new Term("category", "fiction"));
      var rewritten = new TermQuery(new Term("category", "non-fiction"));

      var result = harness.compare(original, rewritten);

      assertFalse(result.describe(), result.equivalent());
      assertEquals(Set.of("doc-a", "doc-b"), result.matchedByOriginal());
      assertEquals(Set.of("doc-c"), result.matchedByRewritten());
      assertEquals(Set.of("doc-a", "doc-b"), result.onlyInOriginal());
      assertEquals(Set.of("doc-c"), result.onlyInRewritten());
    }
  }
}
