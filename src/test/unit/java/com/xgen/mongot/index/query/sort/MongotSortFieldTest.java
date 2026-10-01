package com.xgen.mongot.index.query.sort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonDocument;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class MongotSortFieldTest {
  @DataPoints("outsideScope")
  public static final String[] OUTSIDE_SCOPE = {
    "price", "variants", "variants.details", "variants.detailsExtra.price", "variants.other.price"
  };

  @DataPoints("insideScope")
  public static final String[] INSIDE_SCOPE = {
    "variants.details.price", "variants.details.nested.price"
  };

  @Theory
  public void fromBson_sortFieldOutsideScope_rejected(
      @FromDataPoints("outsideScope") String field) {
    BsonDocument options = scopedOptions();

    BsonParseException exception =
        assertThrows(
            BsonParseException.class,
            () -> MongotSortField.fromBson(field, options, BsonParseContext.root()));

    assertEquals(
        "\"" + field + "\" must be a child of embeddedDocument.path \"variants.details\"",
        exception.getMessage());
  }

  @Theory
  public void fromBson_sortFieldInsideScope_accepted(@FromDataPoints("insideScope") String field)
      throws BsonParseException {
    BsonDocument options = scopedOptions();

    MongotSortField parsed = MongotSortField.fromBson(field, options, BsonParseContext.root());

    assertEquals(FieldPath.parse(field), parsed.field());
    assertEquals(
        FieldPath.parse("variants.details"),
        ((UserFieldSortOptions) parsed.options()).embeddedScope().orElseThrow().path());
  }

  private static BsonDocument scopedOptions() {
    return BsonDocument.parse(
        """
        {
          "order": 1,
          "embeddedDocument": {
            "path": "variants.details",
            "operator": {"exists": {"path": "variants.details.price"}}
          }
        }
        """);
  }
}
