package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.query.operators.HasAncestorOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.TextOperatorBuilder;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class HasAncestorQueryFactoryTest {

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
  public void testHasAncesterQuery() throws Exception {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "teachers",
                FieldDefinitionBuilder.builder()
                    .embeddedDocuments(
                        EmbeddedDocumentsFieldDefinitionBuilder.builder()
                            .field(
                                "students",
                                FieldDefinitionBuilder.builder()
                                    .embeddedDocuments(
                                        EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                            .dynamic(true)
                                            .storedSource(StoredSourceDefinition.createIncludeAll())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    HasAncestorOperator operator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("teachers")
            .operator(TextOperatorBuilder.text().path("teachers.firstName").query("john").build())
            .build();

    Query childQuery =
        new TermQuery(new Term("$embedded:8/teachers/$type:string/teachers.firstName", "john"));

    var embeddedRoot = Optional.of(FieldPath.parse("teachers.students"));
    Query expected =
        BooleanComposer.must(
            new ToChildBlockJoinQuery(
                childQuery, parentFilter(Optional.of(FieldPath.parse("teachers")))),
            new ConstantScoreQuery(
                new TermQuery(new Term("$meta/embeddedPath", "teachers.students"))));
    LuceneSearchTranslation.featureFlagWithMapping(
            FeatureFlags.withQueryFeaturesEnabled(), mappings)
        .assertTranslatedToWithEmbeddedRoot(operator, embeddedRoot, expected);
  }

  @Test
  public void testFailsWhenNotIndexedAsEmbeddedDocuments() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder().dynamic(true).build();

    LuceneSearchTranslation.featureFlagWithMapping(
            FeatureFlags.withQueryFeaturesEnabled(), mappings)
        .assertTranslationThrows(
            OperatorBuilder.hasAncestor()
                .ancestorPath("teachers")
                .operator(
                    TextOperatorBuilder.text().path("teachers.firstName").query("john").build())
                .build());
  }

  @Test
  public void testCompoundOperatorInsideHasAncestorOperator() throws Exception {
    Operator operator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("foo")
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

    String returnScope = "foo.bar";

    Query expected =
        BooleanComposer.must(
            new ToChildBlockJoinQuery(
                childDocumentQueryBuilder.build(),
                parentFilter(Optional.of(FieldPath.parse("foo")))),
            new ConstantScoreQuery(new TermQuery(new Term("$meta/embeddedPath", "foo.bar"))));

    LuceneSearchTranslation.featureFlagWithMapping(
            FeatureFlags.withQueryFeaturesEnabled(),
            DocumentFieldDefinitionBuilder.builder()
                .field(
                    "foo",
                    FieldDefinitionBuilder.builder()
                        .embeddedDocuments(
                            EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
                        .build())
                .field(
                    returnScope,
                    FieldDefinitionBuilder.builder()
                        .embeddedDocuments(
                            EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                .dynamic(true)
                                .storedSource(StoredSourceDefinition.createIncludeAll())
                                .build())
                        .build())
                .build())
        .assertTranslatedToWithEmbeddedRoot(
            operator, Optional.of(FieldPath.parse(returnScope)), expected);
  }

  @Test
  public void testDeeplyNestedMustNotClauseInsideEmbeddedOperator() throws Exception {
    Operator operator =
        OperatorBuilder.compound()
            .mustNot(
                OperatorBuilder.embeddedDocument()
                    .path("foo.bar.baz")
                    .operator(
                        OperatorBuilder.compound()
                            .mustNot(
                                OperatorBuilder.hasAncestor()
                                    .ancestorPath("foo.bar")
                                    .operator(
                                        OperatorBuilder.term()
                                            .path("foo.bar.title")
                                            .query("godfather")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    BooleanQuery.Builder innerCompoundBuilder = new BooleanQuery.Builder();

    innerCompoundBuilder.add(
        BooleanComposer.filterClause(new TermQuery(new Term("$meta/embeddedPath", "foo.bar.baz"))));
    innerCompoundBuilder.add(
        BooleanComposer.mustNotClause(
            BooleanComposer.must(
                new ToChildBlockJoinQuery(
                    new TermQuery(
                        new Term("$embedded:7/foo.bar/$type:string/foo.bar.title", "godfather")),
                    parentFilter(Optional.of(FieldPath.parse("foo.bar")))),
                new ConstantScoreQuery(
                    new TermQuery(new Term("$meta/embeddedPath", "foo.bar.baz"))))));

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

    LuceneSearchTranslation.featureFlagWithMapping(
            FeatureFlags.withQueryFeaturesEnabled(),
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
        .assertTranslatedToWithEmbeddedRoot(operator, Optional.empty(), expected);
  }
}
