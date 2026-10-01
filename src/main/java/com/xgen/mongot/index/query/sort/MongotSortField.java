package com.xgen.mongot.index.query.sort;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonValue;

/**
 * Specifies one component of a {@link SortSpec}: a field and its sort options. Analogous to the
 * Lucene class {@link org.apache.lucene.search.SortField}.
 */
public record MongotSortField(FieldPath field, SortOptions options) {
  public static MongotSortField fromBson(String key, BsonValue value, BsonParseContext context)
      throws BsonParseException {
    var options = SortOptions.fromBson(context, value);
    var field = FieldPath.parse(key);
    if (options instanceof UserFieldSortOptions userOptions
        && userOptions.embeddedScope().isPresent()) {
      var scope = userOptions.embeddedScope().get();
      if (!field.isChildOf(scope.path())) {
        context
            .child(key)
            .handleSemanticError(
                String.format("must be a child of embeddedDocument.path \"%s\"", scope.path()));
      }
    }
    return new MongotSortField(field, options);
  }

  public MongotSortField invert() {
    return new MongotSortField(this.field, this.options.invert());
  }
}
