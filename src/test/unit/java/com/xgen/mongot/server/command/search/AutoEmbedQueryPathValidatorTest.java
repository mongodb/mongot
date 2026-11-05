package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.SearchQueryParseOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddingModelCatalogTestUtils;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import java.util.Arrays;
import java.util.Map;
import org.bson.BsonDocument;
import org.junit.Before;
import org.junit.Test;

public class AutoEmbedQueryPathValidatorTest {

  private static final String MODEL = "voyage-3-large";
  private static final FieldPath DESCRIPTION = FieldPath.parse("description");
  private static final FieldPath META_SUMMARY = FieldPath.parse("meta.summary");
  private static final Map<FieldPath, FieldPath> EMBEDDED_DESCRIPTION =
      Map.of(DESCRIPTION, FieldPath.parse("_autoEmbed.description"));
  private static final Map<FieldPath, FieldPath> EMBEDDED_META_SUMMARY =
      Map.of(META_SUMMARY, FieldPath.parse("_autoEmbed.meta.summary"));
  private static final String VECTOR_SEARCH =
      "{vectorSearch:{path:'description',queryVector:[1.0,2.0],numCandidates:10,limit:3}}";

  @Before
  public void registerModel() {
    EmbeddingModelCatalogTestUtils.registerAutoEmbedModel(MODEL);
  }

  @Test
  public void queriedPaths_operatorsCollectorHighlightAndSort_returnsEveryPath() throws Exception {
    assertPaths(
        "{compound:{must:[{text:{path:['a',{value:'b',multi:'m'}],query:'x'}}],"
            + "mustNot:[{exists:{path:'c'}}],"
            + "should:[{embeddedDocument:{path:'items',"
            + "operator:{text:{path:'items.d',query:'x'}}}}],"
            + "filter:[{queryString:{defaultPath:'e',query:'x'}}]}}",
        "a",
        "b",
        "c",
        "items",
        "items.d",
        "e");
    assertPaths("{moreLikeThis:{like:{f:'x',g:{h:'y'}}}}", "f", "g.h");
    assertPaths("{span:{term:{path:'i',query:'x'}}}", "i");
    assertPaths("{text:{path:'j',query:'x',score:{function:{path:'k'}}}}", "k", "j");
    assertPaths("{exists:{path:'l'},highlight:{path:'m'},sort:{n:1}}", "l", "m", "n");
    assertPaths(
        "{facet:{operator:{exists:{path:'o'}},facets:{p:{type:'string',path:'p'}}}}", "o", "p");
  }

  @Test
  public void queriedPaths_queryString_returnsDefaultPathAndNamedFields() throws Exception {
    assertPaths(
        "{queryString:{defaultPath:'a',"
            + "query:'x AND b:y OR c:[1 TO 2] AND NOT d:z* AND e:\"p q\" AND f:w~'}}",
        "a",
        "b",
        "c",
        "d",
        "e",
        "f");
  }

  @Test
  public void queriedPaths_queryStringMatchAll_returnsNoPath() throws Exception {
    assertPaths("{queryString:{defaultPath:'a',query:'*:*'}}");
  }

  @Test
  public void queriedPaths_queryStringStarFieldOtherThanMatchAll_returnsStarPath()
      throws Exception {
    assertPaths("{queryString:{defaultPath:'a',query:'*:x'}}", "*");
    assertPaths("{queryString:{defaultPath:'a',query:'*:x* OR *:[1 TO 2]'}}", "*");
  }

  @Test
  public void queriedPaths_wildcardPath_returnsNoPath() throws Exception {
    assertPaths(
        "{text:{path:[{wildcard:'*'},{wildcard:'desc*'}],query:'x'},"
            + "highlight:{path:{wildcard:'*'}}}");
  }

  @Test
  public void queriedPaths_fullyQualifiedQueryString_skipsDefaultPath() throws Exception {
    assertPaths("{queryString:{defaultPath:'a',query:'b:x AND c:y'}}", "b", "c");
  }

  @Test
  public void queriedPaths_unparsableQueryString_returnsDefaultPathOnly() throws Exception {
    assertPaths("{queryString:{defaultPath:'a',query:'b:(x'}}", "a");
  }

  @Test
  public void queriedPaths_knnBetaFilter_returnsFilterPaths() throws Exception {
    assertPaths("{knnBeta:{path:'v',vector:[1.0,2.0],k:3,filter:{exists:{path:'w'}}}}", "v", "w");
  }

  @Test
  public void queriedPaths_moreLikeThisArrayOfDocuments_returnsNestedPaths() throws Exception {
    assertPaths("{moreLikeThis:{like:{meta:[{description:'x'}]}}}", "meta.description");
  }

  @Test
  public void queriedPaths_moreLikeThisNonStringValues_returnsOnlyStringPaths() throws Exception {
    assertPaths(
        "{moreLikeThis:{like:{a:'x',b:null,c:5,d:[],e:{},f:{g:1},h:[null,'y']}}}", "a", "h");
  }

  @Test
  public void queriedPaths_metaSort_skipsSortKey() throws Exception {
    assertPaths("{exists:{path:'t'},sort:{u:{$meta:'searchScore'}}}", "t");
  }

  @Test
  public void queriedPaths_vectorSearch_skipsCriteriaPathAndReturnsFilterPaths() throws Exception {
    assertPaths(VECTOR_SEARCH);
    assertPaths(
        "{vectorSearch:{path:'description',queryVector:[1.0,2.0],numCandidates:10,limit:3,"
            + "filter:{exists:{path:'description'}}}}",
        "description");
  }

  @Test
  public void queriedPaths_embeddedScopedFacet_returnsScopePaths() throws Exception {
    assertPaths(
        "{facet:{operator:{exists:{path:'o'}},facets:{p:{type:'string',path:'items.p',"
            + "embeddedDocument:{path:'items',"
            + "operator:{hasRoot:{operator:{exists:{path:'description'}}}}}}}}}",
        "o",
        "items.p",
        "items",
        "description");
  }

  @Test
  public void queriedPaths_embeddedScopedSort_returnsScopePaths() throws Exception {
    SearchQuery query =
        SearchQuery.fromBson(
            BsonDocument.parse(
                "{exists:{path:'t'},sort:{'items.price':{order:1,embeddedDocument:{path:'items',"
                    + "operator:{hasRoot:{operator:{exists:{path:'description'}}}}}}}}"),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            new SearchQueryParseOptions(
                /* allow10k= */ false,
                /* searchAggregationEnabled= */ false,
                /* embeddedScopedSortEnabled= */ true));

    assertThat(AutoEmbedQueryPathValidator.queriedPaths(query))
        .containsExactly(
            FieldPath.parse("t"),
            FieldPath.parse("items.price"),
            FieldPath.parse("items"),
            FieldPath.parse("description"));
  }

  @Test
  public void queriedPaths_aggregationCollector_returnsGroupAndMetricPaths() throws Exception {
    SearchQuery query =
        SearchQuery.fromBson(
            BsonDocument.parse(
                "{facet:{operator:{exists:{path:'q'}},facets:{r:{type:'string',path:'r',"
                    + "aggregations:{total:{type:'sum',path:'s'}}}}}}"),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            new SearchQueryParseOptions(
                /* allow10k= */ false, /* searchAggregationEnabled= */ true));
    assertThat(AutoEmbedQueryPathValidator.queriedPaths(query))
        .containsAtLeast(FieldPath.parse("q"), FieldPath.parse("r"), FieldPath.parse("s"));
  }

  @Test
  public void validate_reservedPathInOperatorFilter_throws() {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryPathValidator.validate(
                    query("{exists:{path:'_autoEmbed.description'}}"),
                    EMBEDDED_DESCRIPTION,
                    served,
                    false));

    assertThat(exception)
        .hasMessageThat()
        .contains(
            "Path '_autoEmbed.description' is reserved for internal automated embeddings fields");
  }

  @Test
  public void validate_reservedPathInNestedFilter_throws() {
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryPathValidator.validate(
                    query(
                        "{compound:{filter:[{exists:{path:'cost'}},"
                            + "{exists:{path:'_autoEmbed'}}]}}"),
                    EMBEDDED_DESCRIPTION,
                    served,
                    false));

    assertThat(exception)
        .hasMessageThat()
        .contains("Path '_autoEmbed' is reserved for internal automated embeddings fields");
  }

  @Test
  public void validate_pathOnlyVectorInDerivedDefinition_throws() throws Exception {
    // A derived definition drops an autoEmbed-only path and holds only its vector.
    SearchIndexDefinition served = definition(derivedDescriptionMappings().build());
    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryPathValidator.validate(
                    query("{exists:{path:'description'}}"), EMBEDDED_DESCRIPTION, served, false));
    assertThat(exception)
        .hasMessageThat()
        .contains("Path 'description' is a type:autoEmbed field and can only be queried");
  }

  @Test
  public void validate_pathServedOnlyAsAutoEmbed_throws() {
    SearchIndexDefinition served =
        definition(
            DocumentFieldDefinitionBuilder.builder()
                .field(
                    "description",
                    FieldDefinitionBuilder.builder()
                        .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                        .build())
                .build());
    assertThrows(
        InvalidQueryException.class,
        () ->
            AutoEmbedQueryPathValidator.validate(
                query("{text:{path:'description',query:'x'}}"),
                EMBEDDED_DESCRIPTION,
                served,
                false));
  }

  @Test
  public void validate_pathServedWithAnotherType_passes() throws Exception {
    SearchIndexDefinition served =
        definition(
            DocumentFieldDefinitionBuilder.builder()
                .field(
                    "description",
                    FieldDefinitionBuilder.builder()
                        .string(StringFieldDefinitionBuilder.builder().build())
                        .build())
                .build());
    AutoEmbedQueryPathValidator.validate(
        query("{exists:{path:'description'}}"), EMBEDDED_DESCRIPTION, served, false);
  }

  @Test
  public void validate_vectorSearchPathOrOtherPath_passes() throws Exception {
    SearchIndexDefinition served = definition(DocumentFieldDefinitionBuilder.builder().build());
    AutoEmbedQueryPathValidator.validate(query(VECTOR_SEARCH), EMBEDDED_DESCRIPTION, served, false);
    AutoEmbedQueryPathValidator.validate(
        query("{exists:{path:'title'}}"), EMBEDDED_DESCRIPTION, served, false);
  }

  @Test
  public void validate_noAutoEmbedPaths_passes() throws Exception {
    SearchIndexDefinition served = definition(DocumentFieldDefinitionBuilder.builder().build());

    AutoEmbedQueryPathValidator.validate(
        query("{text:{path:'description',query:'x'}}"), Map.of(), served, false);
  }

  @Test
  public void validate_queryNotReadingDocumentHoldingAutoEmbedFields_passes() throws Exception {
    SearchIndexDefinition served =
        definition(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "meta",
                    FieldDefinitionBuilder.builder()
                        .document(
                            DocumentFieldDefinitionBuilder.builder()
                                .dynamic(false)
                                .field(
                                    "summary",
                                    FieldDefinitionBuilder.builder()
                                        .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                                        .build())
                                .build())
                        .build())
                .build());

    AutoEmbedQueryPathValidator.validate(
        query("{exists:{path:'title'}}"), EMBEDDED_META_SUMMARY, served, false);
  }

  @Test
  public void validate_pathNotEmbeddedByServedDefinition_passes() throws Exception {
    // The catalog declares description autoEmbed, but the served generation indexes it
    // dynamically.
    SearchIndexDefinition served =
        definition(DocumentFieldDefinitionBuilder.builder().dynamic(true).build());

    AutoEmbedQueryPathValidator.validate(
        query("{text:{path:'description',query:'x'}}"), EMBEDDED_DESCRIPTION, served, false);
  }

  @Test
  public void validate_documentHoldingOnlyAutoEmbedFields_throws() {
    SearchIndexDefinition served =
        definition(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "meta",
                    FieldDefinitionBuilder.builder()
                        .document(
                            DocumentFieldDefinitionBuilder.builder()
                                .dynamic(false)
                                .field(
                                    "summary",
                                    FieldDefinitionBuilder.builder()
                                        .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                                        .build())
                                .build())
                        .build())
                .build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryPathValidator.validate(
                    query("{exists:{path:'meta'}}"), EMBEDDED_META_SUMMARY, served, false));

    assertThat(exception).hasMessageThat().contains("Path 'meta' only contains type:autoEmbed");
  }

  @Test
  public void validate_documentServedWithOtherFields_passes() throws Exception {
    SearchIndexDefinition served =
        definition(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "meta",
                    FieldDefinitionBuilder.builder()
                        .document(
                            DocumentFieldDefinitionBuilder.builder()
                                .dynamic(false)
                                .field(
                                    "title",
                                    FieldDefinitionBuilder.builder()
                                        .string(StringFieldDefinitionBuilder.builder().build())
                                        .build())
                                .build())
                        .build())
                .build());

    AutoEmbedQueryPathValidator.validate(
        query("{exists:{path:'meta'}}"), EMBEDDED_META_SUMMARY, served, false);
  }

  @Test
  public void validate_documentWithOnlyEmptyDocumentAndAutoEmbedFields_throws() {
    SearchIndexDefinition served =
        definition(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "meta",
                    FieldDefinitionBuilder.builder()
                        .document(
                            DocumentFieldDefinitionBuilder.builder()
                                .dynamic(false)
                                .field(
                                    "empty",
                                    FieldDefinitionBuilder.builder()
                                        .document(
                                            DocumentFieldDefinitionBuilder.builder()
                                                .dynamic(false)
                                                .build())
                                        .build())
                                .field(
                                    "summary",
                                    FieldDefinitionBuilder.builder()
                                        .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                                        .build())
                                .build())
                        .build())
                .build());

    InvalidQueryException exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryPathValidator.validate(
                    query("{exists:{path:'meta'}}"), EMBEDDED_META_SUMMARY, served, false));

    assertThat(exception).hasMessageThat().contains("Path 'meta' only contains type:autoEmbed");
  }

  @Test
  public void validate_dynamicDocumentHoldingAutoEmbedField_passes() throws Exception {
    SearchIndexDefinition served =
        definition(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "meta",
                    FieldDefinitionBuilder.builder()
                        .document(
                            DocumentFieldDefinitionBuilder.builder()
                                .dynamic(true)
                                .field(
                                    "summary",
                                    FieldDefinitionBuilder.builder()
                                        .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                                        .build())
                                .build())
                        .build())
                .build());

    AutoEmbedQueryPathValidator.validate(
        query("{exists:{path:'meta'}}"), EMBEDDED_META_SUMMARY, served, false);
  }

  @Test
  public void validate_documentUnderDynamicRoot_passes() throws Exception {
    SearchIndexDefinition served =
        definition(DocumentFieldDefinitionBuilder.builder().dynamic(true).build());

    AutoEmbedQueryPathValidator.validate(
        query("{exists:{path:'meta'}}"), EMBEDDED_META_SUMMARY, served, false);
  }

  @Test
  public void validate_invalidRegexInQueryString_leavesItToQueryExecution() throws Exception {
    SearchIndexDefinition served = definition(descriptionAutoEmbedMappings());

    AutoEmbedQueryPathValidator.validate(
        query("{queryString:{defaultPath:'title',query:'title:/[/'}}"),
        EMBEDDED_DESCRIPTION,
        served,
        false);
  }

  @Test
  public void validate_highlightWithoutDocumentResults_passes() throws Exception {
    SearchIndexDefinition served = definition(descriptionAutoEmbedMappings());

    AutoEmbedQueryPathValidator.validate(
        query(
            "{text:{path:'title',query:'laptop'},count:{type:'total'},"
                + "highlight:{path:'description'}}"),
        EMBEDDED_DESCRIPTION,
        served,
        true);
  }

  @Test
  public void validate_highlightWithDocumentResults_throws() {
    SearchIndexDefinition served = definition(descriptionAutoEmbedMappings());

    assertThrows(
        InvalidQueryException.class,
        () ->
            AutoEmbedQueryPathValidator.validate(
                query("{text:{path:'title',query:'laptop'},highlight:{path:'description'}}"),
                EMBEDDED_DESCRIPTION,
                served,
                false));
  }

  @Test
  public void validate_sortWithoutDocumentResults_passes() throws Exception {
    SearchIndexDefinition served = definition(descriptionAutoEmbedMappings());

    AutoEmbedQueryPathValidator.validate(
        query("{exists:{path:'title'},sort:{description:1}}"), EMBEDDED_DESCRIPTION, served, true);
  }

  @Test
  public void validate_sortWithDocumentResults_throws() {
    SearchIndexDefinition served = definition(descriptionAutoEmbedMappings());

    assertThrows(
        InvalidQueryException.class,
        () ->
            AutoEmbedQueryPathValidator.validate(
                query("{exists:{path:'title'},sort:{description:1}}"),
                EMBEDDED_DESCRIPTION,
                served,
                false));
  }

  @Test
  public void validate_pathAlsoMappedAsDocument_passes() throws Exception {
    SearchIndexDefinition served =
        definition(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "description",
                    FieldDefinitionBuilder.builder()
                        .document(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
                        .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                        .build())
                .build());

    AutoEmbedQueryPathValidator.validate(
        query("{exists:{path:'description'}}"), EMBEDDED_DESCRIPTION, served, false);
  }

  private static DocumentFieldDefinition descriptionAutoEmbedMappings() {
    return DocumentFieldDefinitionBuilder.builder()
        .dynamic(false)
        .field(
            "title",
            FieldDefinitionBuilder.builder()
                .string(StringFieldDefinitionBuilder.builder().build())
                .build())
        .field(
            "description",
            FieldDefinitionBuilder.builder()
                .searchAutoEmbed(new SearchAutoEmbedFieldDefinition(MODEL))
                .build())
        .build();
  }

  private static void assertPaths(String queryJson, String... expected) throws Exception {
    assertThat(AutoEmbedQueryPathValidator.queriedPaths(query(queryJson)))
        .containsExactlyElementsIn(Arrays.stream(expected).map(FieldPath::parse).toList());
  }

  private static SearchQuery query(String queryJson) {
    try {
      return SearchQuery.fromBson(BsonDocument.parse(queryJson));
    } catch (BsonParseException e) {
      throw new AssertionError(e);
    }
  }

  private static DocumentFieldDefinitionBuilder derivedDescriptionMappings() {
    return DocumentFieldDefinitionBuilder.builder()
        .dynamic(false)
        .field(
            "_autoEmbed",
            FieldDefinitionBuilder.builder()
                .document(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(false)
                        .field(
                            "description",
                            FieldDefinitionBuilder.builder()
                                .searchIndexVectorField(
                                    new SearchIndexVectorFieldDefinition(
                                        new VectorFieldSpecification(
                                            2,
                                            VectorSimilarity.DOT_PRODUCT,
                                            VectorQuantization.NONE,
                                            new VectorIndexingAlgorithm.HnswIndexingAlgorithm())))
                                .build())
                        .build())
                .build());
  }

  private static SearchIndexDefinition definition(DocumentFieldDefinition mappings) {
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .mappings(mappings)
        .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
        .build();
  }
}
