package com.xgen.mongot.util.bson.parser;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public final class DateUtil {
  /**
   * ISO-8601 / RFC3339-style instant formatter at whole-second precision
   * (e.g. 2026-04-13T19:20:30Z), using the GMT zone.
   */
  public static final DateTimeFormatter GMT_DATETIME_SECONDS_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.of("GMT"));

  public static Optional<Instant> parseInstantFromString(
      DocumentParser parser, DateTimeFormatter formatter, Field.Optional<String> field)
      throws BsonParseException {
    try {
      Optional<String> definitionCreatedAtAsString = parser.getField(field).unwrap();
      return definitionCreatedAtAsString.map(string -> Instant.from(formatter.parse(string)));
    } catch (DateTimeParseException e) {
      return parser
          .getContext()
          .child(field.getName())
          .handleSemanticError(String.format("could not be parsed: %s", e.getMessage()));
    }
  }
}
