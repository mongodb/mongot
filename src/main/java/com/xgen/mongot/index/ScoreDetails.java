package com.xgen.mongot.index;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.bson.BsonDocument;

public class ScoreDetails implements DocumentEncodable, Comparable<ScoreDetails> {
  public static class Fields {
    public static final Field.Required<Float> VALUE =
        Field.builder("value").floatField().required();

    public static final Field.Required<String> DESCRIPTION =
        Field.builder("description").stringField().required();

    public static final Field.WithDefault<List<ScoreDetails>> DETAILS =
        Field.builder("details")
            .classField(ScoreDetails::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .withDefault(Collections.emptyList());
  }

  private final float value;
  private final String description;
  private final List<ScoreDetails> details;

  public ScoreDetails(float value, String description, List<ScoreDetails> details) {
    this.value = value;
    this.description = description;
    this.details = details;
  }

  public static ScoreDetails fromBson(DocumentParser parser) throws BsonParseException {
    return new ScoreDetails(
        parser.getField(Fields.VALUE).unwrap(),
        parser.getField(Fields.DESCRIPTION).unwrap(),
        parser.getField(Fields.DETAILS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.VALUE, this.value)
        .field(Fields.DESCRIPTION, this.description)
        .field(Fields.DETAILS, this.details)
        .build();
  }

  public float getValue() {
    return this.value;
  }

  public String getDescription() {
    return this.description;
  }

  public List<ScoreDetails> getDetails() {
    return this.details;
  }

  /**
   * This method is used to establish a consistent ordering for comparing expected vs. actual score
   * details output during testing. We sort the list of details by their descriptions.
   */
  @VisibleForTesting
  ScoreDetails sorted() {
    if (this.details.isEmpty()) {
      return this;
    }

    // Sort each of the sub score details if there are any
    List<ScoreDetails> sortedDetails =
        this.details.stream()
            .map(ScoreDetails::sorted) // ensure each detail is sorted
            .sorted(ScoreDetails::compareTo) // sort the list of details
            .collect(Collectors.toUnmodifiableList());

    return new ScoreDetails(this.value, this.description, sortedDetails);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ScoreDetails)) {
      return false;
    }
    ScoreDetails that = (ScoreDetails) o;
    return Float.compare(that.value, this.value) == 0
        && this.description.equals(that.description)
        && this.details.equals(that.details);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.value, this.description, this.details);
  }

  @Override
  public int compareTo(ScoreDetails o) {
    return this.description.compareTo(o.getDescription());
  }

  @Override
  public String toString() {
    return toBson().toJson();
  }
}
