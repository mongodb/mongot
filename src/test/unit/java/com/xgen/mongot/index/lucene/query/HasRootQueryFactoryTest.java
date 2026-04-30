package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.query.operators.HasRootOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.TextOperatorBuilder;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class HasRootQueryFactoryTest {

  @Test
  public void testHasRootQuery() throws Exception {
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
            .field(
                "inspector",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    HasRootOperator operator =
        OperatorBuilder.hasRoot()
            .operator(TextOperatorBuilder.text().path("inspector").query("john").build())
            .build();

    Query childQuery = new TermQuery(new Term("$type:string/inspector", "john"));

    var embeddedRoot = Optional.of(FieldPath.parse("teachers.students"));
    Query expected =
        BooleanComposer.must(
            new ToChildBlockJoinQuery(childQuery, HasRootQueryFactory.ROOT_BIT_SET_PRODUCER),
            new ConstantScoreQuery(
                new TermQuery(new Term("$meta/embeddedPath", "teachers.students"))));

    LuceneSearchTranslation.featureFlagWithMapping(
            FeatureFlags.withQueryFeaturesEnabled(), mappings)
        .assertTranslatedToWithEmbeddedRoot(operator, embeddedRoot, expected);
  }

  @Test
  public void testMustNotClauseInsideHasRootOperator() throws Exception {
    Operator operator =
        OperatorBuilder.hasRoot()
            .operator(
                OperatorBuilder.compound()
                    .mustNot(OperatorBuilder.term().path("foo.title").query("godfather").build())
                    .build())
            .build();

    BooleanQuery.Builder childDocumentQueryBuilder = new BooleanQuery.Builder();

    childDocumentQueryBuilder.add(
        BooleanComposer.filterClause(new TermQuery(new Term("$meta/embeddedRoot", "T"))));
    childDocumentQueryBuilder.add(
        BooleanComposer.mustNotClause(
            new TermQuery(new Term("$type:string/foo.title", "godfather"))));

    String returnScope = "foo.bar";

    Query expected =
        BooleanComposer.must(
            new ToChildBlockJoinQuery(
                childDocumentQueryBuilder.build(), HasRootQueryFactory.ROOT_BIT_SET_PRODUCER),
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
                                OperatorBuilder.hasRoot()
                                    .operator(
                                        OperatorBuilder.term()
                                            .path("random")
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
                    new TermQuery(new Term("$type:string/random", "godfather")),
                    HasRootQueryFactory.ROOT_BIT_SET_PRODUCER),
                new ConstantScoreQuery(
                    new TermQuery(new Term("$meta/embeddedPath", "foo.bar.baz"))))));

    // Inner block join query joins to parent embedded documents at path "foo".
    Query fooEmbeddedDocumentsQuery =
        new WrappedToParentBlockJoinQuery(
            new DisableBulkScorerQuery(innerCompoundBuilder.build()),
            HasRootQueryFactory.ROOT_BIT_SET_PRODUCER,
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
                .field(
                    "random",
                    FieldDefinitionBuilder.builder()
                        .string(StringFieldDefinitionBuilder.builder().build())
                        .build())
                .build())
        .assertTranslatedToWithEmbeddedRoot(operator, Optional.empty(), expected);
  }
}
