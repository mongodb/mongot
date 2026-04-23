package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.bson.BsonDocument;

/**
 * Options for vector search queries on embedded vector fields.
 *
 * <p>When querying embedded vectors (vectors within array subdocuments), multiple child vectors may
 * match the query. The {@code scoreMode} determines how to aggregate scores from matching child
 * documents to compute a single score for the parent document.
 */
public record VectorEmbeddedOptions(ScoreMode scoreMode) implements DocumentEncodable {

  /**
   * Defines how to aggregate scores from multiple matching child documents.
   *
   * <ul>
   *   <li>{@code MAX} - Use the maximum score from all matching child documents
   *   <li>{@code AVG} - Use the average score from all matching child documents
   * </ul>
   */
  public enum ScoreMode {
    MAX,
    AVG
  }

  // Case-sensitive lookup from the lowercase wire representation (e.g. "max") to the enum
  private static final Map<String, ScoreMode> BY_WIRE_NAME =
      Arrays.stream(ScoreMode.values())
          .collect(
              Collectors.toUnmodifiableMap(
                  m -> m.name().toLowerCase(Locale.ROOT),
                  Function.identity(),
                  (a, b) -> {
                    throw new IllegalStateException("duplicate ScoreMode wire name: " + a);
                  }));

  public static class Fields {
    public static final Field.WithDefault<String> SCORE_MODE =
        Field.builder("scoreMode")
            .stringField()
            .optional()
            .withDefault(ScoreMode.MAX.name().toLowerCase(Locale.ROOT));
  }

  public static VectorEmbeddedOptions fromBson(DocumentParser parser) throws BsonParseException {
    String value = parser.getField(Fields.SCORE_MODE).unwrap();
    ScoreMode mode = BY_WIRE_NAME.get(value);
    if (mode == null) {
      throw new BsonParseException(
          String.format(
              "scoreMode: \"%s\" is not supported. Accepted values are %s",
              value, formatAcceptedValues()),
          Optional.empty());
    }
    return new VectorEmbeddedOptions(mode);
  }

  private static String formatAcceptedValues() {
    String[] quoted =
        Arrays.stream(ScoreMode.values())
            .map(m -> "\"" + m.name().toLowerCase(Locale.ROOT) + "\"")
            .toArray(String[]::new);
    if (quoted.length <= 2) {
      return String.join(" or ", quoted);
    }
    return String.join(", ", Arrays.copyOf(quoted, quoted.length - 1))
        + ", or "
        + quoted[quoted.length - 1];
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.SCORE_MODE, this.scoreMode.name().toLowerCase(Locale.ROOT))
        .build();
  }
}
