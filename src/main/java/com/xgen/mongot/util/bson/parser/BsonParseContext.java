package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.SingleLinkedList;
import java.util.Collection;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.bson.BsonType;
import org.jetbrains.annotations.Nullable;

public class BsonParseContext {

  private static final BsonParseContext ROOT = new BsonParseContext(SingleLinkedList.empty());

  private final SingleLinkedList<String> hierarchy;

  BsonParseContext(SingleLinkedList<String> ancestors) {
    this.hierarchy = ancestors;
  }

  public static BsonParseContext root() {
    return ROOT;
  }

  public BsonParseContext child(String segment) {
    return new BsonParseContext(this.hierarchy.prepend(segment));
  }

  /** context for array element. */
  public BsonParseContext arrayElement(int index) {
    // Add [index] to path without adding a delimiter before it.
    if (this.hierarchy.isEmpty()) {
      return new BsonParseContext(this.hierarchy.prepend("[" + index + ']'));
    } else {
      String elementPath = this.hierarchy.head() + "[" + index + ']';
      return new BsonParseContext(this.hierarchy.getTail().prepend(elementPath));
    }
  }

  /** Throws a BsonParseException describing overflow. */
  public void handleOverflow(@Nullable String typeDescription) throws BsonParseException {
    throw deserializationException(String.format("overflowed, must fit in a %s", typeDescription));
  }

  /** Throws a BsonParseException describing the semantic error. */
  public <T> T handleSemanticError(@Nullable String description) throws BsonParseException {
    throw deserializationException(description);
  }

  /** Throws a BsonParseException describing the unexpected type. */
  public <T> T handleUnexpectedType(String expected, BsonType actual) throws BsonParseException {
    if (actual == BsonType.NULL) {
      throw deserializationException("is required");
    }

    throw deserializationException(String.format("must be a %s", expected));
  }

  /**
   * Throws a BsonParseException describing the unexpected fields. {@code expectedFields} are the
   * fields the parser recognized for this document (used to make typos like {@code
   * embeddedDocuments} vs {@code embeddedDocument} obvious); when non-empty they are appended to
   * the message.
   */
  public void handleUnexpectedFields(Collection<String> fields, Collection<String> expectedFields)
      throws BsonParseException {
    String expectedClause =
        expectedFields.isEmpty()
            ? ""
            : ". Expected fields are: "
                + expectedFields.stream().sorted().collect(Collectors.joining(", "));

    if (fields.size() == 1) {
      throw deserializationException(
          String.format("unrecognized field \"%s\"%s", fields.iterator().next(), expectedClause));
    }

    throw deserializationException(
        String.format(
            "unrecognized fields [%s]%s",
            fields.stream()
                .sorted()
                .map(s -> String.format("\"%s\"", s))
                .collect(Collectors.joining(", ")),
            expectedClause));
  }

  /** Throws a BsonParseException describing the underflow. */
  public void handleUnderflow(String typeDescription) throws BsonParseException {
    throw deserializationException(String.format("underflowed, must fit in a %s", typeDescription));
  }

  public BsonParseException deserializationException(@Nullable String message) {
    if (this.hierarchy.isEmpty()) {
      return new BsonParseException(message, Optional.empty());
    }
    StringJoiner result =
        this.hierarchy.foldRight(new StringJoiner(FieldPath.DELIMITER), StringJoiner::add);
    // Use parse rather than fromParts here because segments may contain dots.
    FieldPath path = FieldPath.parse(result.toString());
    return new BsonParseException(message, Optional.of(path));
  }

  public SingleLinkedList<String> getHierarchy() {
    return this.hierarchy;
  }
}
