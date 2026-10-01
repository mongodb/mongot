package com.xgen.mongot.index.query;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SearchQueryTest {
  private final String sortField;
  private final QueryOptimizationFlags queryOptimizationFlags;

  public SearchQueryTest(String sortField, boolean omitSearchDocumentResults) {
    this.sortField = sortField;
    this.queryOptimizationFlags = new QueryOptimizationFlags(omitSearchDocumentResults);
  }

  @Parameterized.Parameters(name = "sortField={0}, omitSearchDocumentResults={1}")
  public static List<Object[]> data() {
    return List.of(
        new Object[] {"sort", false},
        new Object[] {"sort", true},
        new Object[] {"sortBetaV1", false},
        new Object[] {"sortBetaV1", true});
  }

  @Test
  public void fromBson_scopedOperatorSort_rejected() {
    BsonDocument document =
        BsonDocument.parse(
                """
                {"exists": {"path": "variants.price"}}
                """)
            .append(this.sortField, scopedSort());

    BsonParseException exception =
        assertThrows(
            BsonParseException.class,
            () -> SearchQuery.fromBson(document, this.queryOptimizationFlags));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "\""
                + this.sortField
                + ".variants.price.embeddedDocument\" is not yet supported for sorting");
  }

  @Test
  public void fromBson_scopedCollectorSort_rejected() {
    BsonDocument document =
        BsonDocument.parse(
                """
                {"facet": {"facets": {"categories": {"type": "string", "path": "category"}}}}
                """)
            .append(this.sortField, scopedSort());

    BsonParseException exception =
        assertThrows(
            BsonParseException.class,
            () -> SearchQuery.fromBson(document, this.queryOptimizationFlags));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "\""
                + this.sortField
                + ".variants.price.embeddedDocument\" is not yet supported for sorting");
  }

  private static BsonDocument scopedSort() {
    return BsonDocument.parse(
        """
        {
          "_id": 1,
          "variants.price": {
            "order": 1,
            "embeddedDocument": {
              "path": "variants",
              "operator": {"equals": {"path": "variants.inStock", "value": true}}
            }
          }
        }
        """);
  }
}
