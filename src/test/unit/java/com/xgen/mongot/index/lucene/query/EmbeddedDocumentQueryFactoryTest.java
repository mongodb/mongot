package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.scores.EmbeddedScore;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.EmbeddedDocumentOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.TextOperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.EmbeddedScoreBuilder;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class EmbeddedDocumentQueryFactoryTest {

  private static BitSetProducer parentFilter(Optional<FieldPath> embeddedRoot) {
    return embeddedRoot
        .map(
            root ->
                new QueryBitSetProducer(
                    new TermQuery(new Term("$meta/embeddedPath", root.toString()))))
        .orElseGet(
            () -> new QueryBitSetProducer(new TermQuery(new Term("$meta/embeddedRoot", "T"))));
  }

  @Test
  public void testSingleEmbeddedDocumentQueryDefaultScore() throws Exception {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "teachers",
                FieldDefinitionBuilder.builder()
                    .embeddedDocuments(
                        EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
                    .build())
            .build();

    EmbeddedDocumentOperator operator =
        EmbeddedDocumentOperatorBuilder.embeddedDocument()
            .path("teachers")
            .operator(TextOperatorBuilder.text().path("teachers.firstName").query("john").build())
            .build();

    Query childQuery =
        new TermQuery(new Term("$embedded:8/teachers/$type:string/teachers.firstName", "john"));

    Query expected =
        new WrappedToParentBlockJoinQuery(
            new DisableBulkScorerQuery(childQuery),
            parentFilter(Optional.empty()),
            ScoreMode.Total);

    LuceneSearchTranslation.mapped(mappings).assertTranslatedTo(operator, expected);
  }

  @Test
  public void testSingleEmbeddedDocumentQueryDoesNotWrapWhenDynamicFlagExplicitlyDisabled()
      throws Exception {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "teachers",
                FieldDefinitionBuilder.builder()
                    .embeddedDocuments(
                        EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
                    .build())
            .build();

    EmbeddedDocumentOperator operator =
        EmbeddedDocumentOperatorBuilder.embeddedDocument()
            .path("teachers")
            .operator(TextOperatorBuilder.text().path("teachers.firstName").query("john").build())
            .build();

    Query childQuery =
        new TermQuery(new Term("$embedded:8/teachers/$type:string/teachers.firstName", "john"));

    Query expected =
        new WrappedToParentBlockJoinQuery(
            childQuery, parentFilter(Optional.empty()), ScoreMode.Total);

    LuceneSearchTranslation.mappedWithDisableBulkScorerQueryForEmbeddedDocumentChildDisabled(
            mappings)
        .assertTranslatedTo(operator, expected);
  }

  static class AggregatePairing {
    final EmbeddedScore.Aggregate aggregate;
    final ScoreMode scoreMode;

    AggregatePairing(EmbeddedScore.Aggregate aggregate, ScoreMode scoreMode) {
      this.aggregate = aggregate;
      this.scoreMode = scoreMode;
    }

    static AggregatePairing of(EmbeddedScore.Aggregate aggregate, ScoreMode scoreMode) {
      return new AggregatePairing(aggregate, scoreMode);
    }
  }

  @DataPoints("aggregate")
  public static AggregatePairing[] aggregate() {
    return new AggregatePairing[] {
      AggregatePairing.of(EmbeddedScore.Aggregate.MEAN, ScoreMode.Avg),
      AggregatePairing.of(EmbeddedScore.Aggregate.MAXIMUM, ScoreMode.Max),
      AggregatePairing.of(EmbeddedScore.Aggregate.MINIMUM, ScoreMode.Min),
      AggregatePairing.of(EmbeddedScore.Aggregate.SUM, ScoreMode.Total)
    };
  }

  @Theory
  public void testSingleEmbeddedDocumentQuerySpecificScore(
      @FromDataPoints("aggregate") AggregatePairing aggregatePairing) throws Exception {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "teachers",
                FieldDefinitionBuilder.builder()
                    .embeddedDocuments(
                        EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
                    .build())
            .build();

    EmbeddedDocumentOperator operator =
        EmbeddedDocumentOperatorBuilder.embeddedDocument()
            .path("teachers")
            .operator(TextOperatorBuilder.text().path("teachers.firstName").query("john").build())
            .score(new EmbeddedScoreBuilder().aggregate(aggregatePairing.aggregate).build())
            .build();

    Query childQuery =
        new TermQuery(new Term("$embedded:8/teachers/$type:string/teachers.firstName", "john"));

    Query expected =
        new WrappedToParentBlockJoinQuery(
            new DisableBulkScorerQuery(childQuery),
            parentFilter(Optional.empty()),
            aggregatePairing.scoreMode);

    LuceneSearchTranslation.mapped(mappings).assertTranslatedTo(operator, expected);
  }

  @Test
  public void testFailsWhenNotIndexedAsEmbeddedDocuments() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder().dynamic(true).build();

    LuceneSearchTranslation.mapped(mappings)
        .assertTranslationThrows(
            EmbeddedDocumentOperatorBuilder.embeddedDocument()
                .path("teachers")
                .operator(
                    TextOperatorBuilder.text().path("teachers.firstName").query("john").build())
                .build());
  }

  @Test
  public void testMustNotClauseInsideEmbeddedOperator() throws Exception {
    Operator operator =
        OperatorBuilder.embeddedDocument()
            .path("foo")
            .operator(
                OperatorBuilder.compound()
                    .mustNot(OperatorBuilder.term().path("foo.title").query("godfather").build())
                    .build())
            .build();

    BooleanQuery.Builder childDocumentQueryBuilder = new BooleanQuery.Builder();

    childDocumentQueryBuilder.add(
        BooleanComposer.filterClause(new TermQuery(new Term("$meta/embeddedPath", "foo"))));
    childDocumentQueryBuilder.add(
        BooleanComposer.mustNotClause(
            new TermQuery(new Term("$embedded:3/foo/$type:string/foo.title", "godfather"))));

    Query expected =
        new WrappedToParentBlockJoinQuery(
            new DisableBulkScorerQuery(childDocumentQueryBuilder.build()),
            parentFilter(Optional.empty()),
            ScoreMode.Total);

    LuceneSearchTranslation.mapped(
            DocumentFieldDefinitionBuilder.builder()
                .field(
                    "foo",
                    FieldDefinitionBuilder.builder()
                        .embeddedDocuments(
                            EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
                        .build())
                .build())
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testDeeplyNestedMustNotClauseInsideEmbeddedOperator() throws Exception {
    /*
       {
         compound: {
           mustNot: {
             embeddedDocument: {
               path: "foo",
               operator: {
                 compound: {
                   mustNot: {
                     embeddedDocument: {
                       path: "foo.bar.baz",
                       operator: {
                         text: {
                           path: "foo.bar.baz.title",
                           query: "godfather"
                         }
                       }
                     }
                   }
                 }
               }
             }
           }
         }
       }
    */
    Operator operator =
        OperatorBuilder.compound()
            .mustNot(
                OperatorBuilder.embeddedDocument()
                    .path("foo")
                    .operator(
                        OperatorBuilder.compound()
                            .mustNot(
                                OperatorBuilder.embeddedDocument()
                                    .path("foo.bar.baz")
                                    .operator(
                                        OperatorBuilder.term()
                                            .path("foo.bar.baz.title")
                                            .query("godfather")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    BooleanQuery.Builder innerCompoundBuilder = new BooleanQuery.Builder();

    // Inner compound mustNot clause should have filter applied that only selects documents at
    // embedded path foo; mustNot clause is scoped to fields and embedded document children with a
    // parent at embedded path "foo".
    innerCompoundBuilder.add(
        BooleanComposer.filterClause(new TermQuery(new Term("$meta/embeddedPath", "foo"))));
    innerCompoundBuilder.add(
        BooleanComposer.mustNotClause(
            new WrappedToParentBlockJoinQuery(
                new DisableBulkScorerQuery(
                    new TermQuery(
                        new Term(
                            "$embedded:11/foo.bar.baz/$type:string/foo.bar.baz.title",
                            "godfather"))),
                parentFilter(Optional.of(FieldPath.parse("foo"))),
                ScoreMode.Total)));

    // Inner block join query joins to parent embedded documents at path "foo".
    Query fooEmbeddedDocumentsQuery =
        new WrappedToParentBlockJoinQuery(
            new DisableBulkScorerQuery(innerCompoundBuilder.build()),
            parentFilter(Optional.empty()),
            ScoreMode.Total);

    BooleanQuery.Builder outerCompoundBuilder = new BooleanQuery.Builder();

    // Outer compound mustNot clause should have filter applied that only selects documents at
    // embedded root.
    outerCompoundBuilder.add(
        BooleanComposer.filterClause(new TermQuery(new Term("$meta/embeddedRoot", "T"))));
    outerCompoundBuilder.add(BooleanComposer.mustNotClause(fooEmbeddedDocumentsQuery));

    Query expected = outerCompoundBuilder.build();

    /*
     {
       fields: {
         foo: {
           type: "embeddedDocuments",
           fields: {
             bar: {
               type: "embeddedDocuments",
               fields: {
                 baz: {
                   type: "embeddedDocuments",
                   dynamic: true
                 }
               }
             }
           }
         }
       },
     }
    */
    LuceneSearchTranslation.mapped(
            DocumentFieldDefinitionBuilder.builder()
                .field(
                    "foo",
                    FieldDefinitionBuilder.builder()
                        .embeddedDocuments(
                            EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                .field(
                                    "bar",
                                    FieldDefinitionBuilder.builder()
                                        .embeddedDocuments(
                                            EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                                .field(
                                                    "baz",
                                                    FieldDefinitionBuilder.builder()
                                                        .embeddedDocuments(
                                                            EmbeddedDocumentsFieldDefinitionBuilder
                                                                .builder()
                                                                .dynamic(true)
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .assertTranslatedTo(operator, expected);
  }
}
