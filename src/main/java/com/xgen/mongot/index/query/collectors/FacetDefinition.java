package com.xgen.mongot.index.query.collectors;

import com.xgen.mongot.util.bson.BsonNumberUtils;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldValidator;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.commons.lang3.Range;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonValue;

public sealed interface FacetDefinition extends DocumentEncodable
    permits FacetDefinition.StringFacetDefinition, FacetDefinition.BoundaryFacetDefinition {

  class Fields {
    private static final Field.Required<Type> TYPE =
        Field.builder("type").enumField(Type.class).asCamelCase().required();

    private static final Field.Required<String> PATH =
        Field.builder("path").stringField().required();

    static final Field.Optional<EmbeddedDocumentScope> EMBEDDED_DOCUMENT =
        Field.builder("embeddedDocument")
            .classField(EmbeddedDocumentScope::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  enum Type {
    STRING,
    NUMBER,
    DATE
  }

  /** Deserializes definition from a BSON (1k numBuckets limit for string facets). */
  static FacetDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return fromBson(parser, false);
  }

  /**
   * Deserializes definition from a BSON.
   *
   * <p>Must not close the parser: callers may read additional fields from the same document
   * afterward (e.g. {@link AggregationFacetDefinition#fromBson} reads {@code aggregations} and
   * nested {@code facets} on top of the group payload parsed here).
   *
   * @param allow10k when true, string facet {@code numBuckets} may be 1–10_000 and number/date
   *     facet {@code boundaries} may contain 2–10_000 values; when false, each limit is 1_000
   *     (parser throws BsonParseException if exceeded).
   */
  static FacetDefinition fromBson(DocumentParser parser, boolean allow10k)
      throws BsonParseException {
    return switch (parser.getField(Fields.TYPE).unwrap()) {
      case STRING -> new StringFacetDefinition(parser, allow10k);
      case NUMBER -> new NumericFacetDefinition(parser, allow10k);
      case DATE -> new DateFacetDefinition(parser, allow10k);
    };
  }

  String path();

  Type getType();

  /**
   * The inline embedded-document scope for this facet, if any. When present, the facet counts only
   * children matching {@link EmbeddedDocumentScope#operator()}, deduplicated to at most one count
   * per root document per bucket. When absent, the facet is document-indexed as today.
   */
  default Optional<EmbeddedDocumentScope> embeddedDocument() {
    return Optional.empty();
  }

  record StringFacetDefinition(
      String path, int numBuckets, Optional<EmbeddedDocumentScope> embeddedDocument)
      implements FacetDefinition {
    private static class Fields {
      private static final Field.WithDefault<Integer> NUM_BUCKETS_1K =
          Field.builder("numBuckets")
              .intField()
              .mustBeWithinBounds(Range.of(1, 1000))
              .optional()
              .withDefault(10);
      private static final Field.WithDefault<Integer> NUM_BUCKETS_10K =
          Field.builder("numBuckets")
              .intField()
              .mustBeWithinBounds(Range.of(1, 10_000))
              .optional()
              .withDefault(10);
    }

    /** Backwards-compatible constructor for document-indexed (unscoped) string facets. */
    public StringFacetDefinition(String path, int numBuckets) {
      this(path, numBuckets, Optional.empty());
    }

    private StringFacetDefinition(DocumentParser parser, boolean allow10k)
        throws BsonParseException {
      this(
          parser.getField(FacetDefinition.Fields.PATH).unwrap(),
          parser.getField(allow10k ? Fields.NUM_BUCKETS_10K : Fields.NUM_BUCKETS_1K).unwrap(),
          parser.getField(FacetDefinition.Fields.EMBEDDED_DOCUMENT).unwrap());
    }

    @Override
    public Type getType() {
      return Type.STRING;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(FacetDefinition.Fields.TYPE, this.getType())
          .field(FacetDefinition.Fields.PATH, this.path())
          .field(Fields.NUM_BUCKETS_10K, this.numBuckets())
          .field(FacetDefinition.Fields.EMBEDDED_DOCUMENT, this.embeddedDocument())
          .build();
    }
  }

  sealed interface BoundaryFacetDefinition<T extends BsonValue> extends FacetDefinition
      permits NumericFacetDefinition, DateFacetDefinition {

    class Fields {
      private static final Field.Optional<String> DEFAULT =
          Field.builder("default").stringField().mustNotBeEmpty().optional().noDefault();
    }

    /** Returns a list of values that specify the boundaries for each bucket. */
    List<T> boundaries();

    /**
     * Returns the name of an additional bucket that counts documents returned from the operator
     * that do not fall within the specified boundaries.
     */
    Optional<String> defaultBucketName();
  }

  record NumericFacetDefinition(
      String path,
      Optional<String> defaultBucketName,
      List<BsonNumber> boundaries,
      Optional<EmbeddedDocumentScope> embeddedDocument)
      implements BoundaryFacetDefinition<BsonNumber> {

    private static class Validators {
      private static final FieldValidator<List<BsonNumber>> BOUNDARIES_ORDER_VALIDATOR =
          numbers -> {
            var inOrder =
                IntStream.range(0, numbers.size() - 1)
                    .allMatch(i -> BsonNumberUtils.compare(numbers.get(i), numbers.get(i + 1)) < 0);
            return inOrder
                ? Optional.empty()
                : Optional.of("must be distinct numbers in ascending order");
          };
    }

    private static class Fields {
      private static final Field.Required<List<BsonNumber>> BOUNDARIES_1K =
          Field.builder("boundaries")
              .bsonNumberField()
              .asList()
              .sizeMustBeWithinBounds(Range.of(2, 1000))
              .validate(Validators.BOUNDARIES_ORDER_VALIDATOR)
              .required();
      private static final Field.Required<List<BsonNumber>> BOUNDARIES_10K =
          Field.builder("boundaries")
              .bsonNumberField()
              .asList()
              .sizeMustBeWithinBounds(Range.of(2, 10_000))
              .validate(Validators.BOUNDARIES_ORDER_VALIDATOR)
              .required();
    }

    /** Backwards-compatible constructor for document-indexed (unscoped) number facets. */
    public NumericFacetDefinition(
        String path, Optional<String> defaultBucketName, List<BsonNumber> boundaries) {
      this(path, defaultBucketName, boundaries, Optional.empty());
    }

    private NumericFacetDefinition(DocumentParser parser, boolean allow10k)
        throws BsonParseException {
      this(
          parser.getField(FacetDefinition.Fields.PATH).unwrap(),
          parser.getField(BoundaryFacetDefinition.Fields.DEFAULT).unwrap(),
          parser.getField(allow10k ? Fields.BOUNDARIES_10K : Fields.BOUNDARIES_1K).unwrap(),
          parser.getField(FacetDefinition.Fields.EMBEDDED_DOCUMENT).unwrap());
    }

    @Override
    public Type getType() {
      return Type.NUMBER;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(FacetDefinition.Fields.TYPE, this.getType())
          .field(FacetDefinition.Fields.PATH, this.path())
          .field(BoundaryFacetDefinition.Fields.DEFAULT, this.defaultBucketName())
          .field(Fields.BOUNDARIES_10K, this.boundaries())
          .field(FacetDefinition.Fields.EMBEDDED_DOCUMENT, this.embeddedDocument())
          .build();
    }
  }

  record DateFacetDefinition(
      String path,
      Optional<String> defaultBucketName,
      List<BsonDateTime> boundaries,
      Optional<EmbeddedDocumentScope> embeddedDocument)
      implements BoundaryFacetDefinition<BsonDateTime> {

    private static class Validators {
      private static final FieldValidator<List<BsonDateTime>> BOUNDARIES_ORDER_VALIDATOR =
          dates -> {
            var inOrder =
                IntStream.range(0, dates.size() - 1)
                    .allMatch(i -> dates.get(i).compareTo(dates.get(i + 1)) < 0);
            return inOrder
                ? Optional.empty()
                : Optional.of("must be distinct dates in ascending order");
          };
    }

    private static class Fields {
      private static final Field.Required<List<BsonDateTime>> BOUNDARIES_1K =
          Field.builder("boundaries")
              .listOf(Value.builder().bsonDateTimeField().required())
              .sizeMustBeWithinBounds(Range.of(2, 1000))
              .validate(Validators.BOUNDARIES_ORDER_VALIDATOR)
              .required();
      private static final Field.Required<List<BsonDateTime>> BOUNDARIES_10K =
          Field.builder("boundaries")
              .listOf(Value.builder().bsonDateTimeField().required())
              .sizeMustBeWithinBounds(Range.of(2, 10_000))
              .validate(Validators.BOUNDARIES_ORDER_VALIDATOR)
              .required();
    }

    /** Backwards-compatible constructor for document-indexed (unscoped) date facets. */
    public DateFacetDefinition(
        String path, Optional<String> defaultBucketName, List<BsonDateTime> boundaries) {
      this(path, defaultBucketName, boundaries, Optional.empty());
    }

    private DateFacetDefinition(DocumentParser parser, boolean allow10k) throws BsonParseException {
      this(
          parser.getField(FacetDefinition.Fields.PATH).unwrap(),
          parser.getField(BoundaryFacetDefinition.Fields.DEFAULT).unwrap(),
          parser.getField(allow10k ? Fields.BOUNDARIES_10K : Fields.BOUNDARIES_1K).unwrap(),
          parser.getField(FacetDefinition.Fields.EMBEDDED_DOCUMENT).unwrap());
    }

    @Override
    public Type getType() {
      return Type.DATE;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(FacetDefinition.Fields.TYPE, this.getType())
          .field(FacetDefinition.Fields.PATH, this.path())
          .field(BoundaryFacetDefinition.Fields.DEFAULT, this.defaultBucketName())
          .field(Fields.BOUNDARIES_10K, this.boundaries())
          .field(FacetDefinition.Fields.EMBEDDED_DOCUMENT, this.embeddedDocument())
          .build();
    }
  }
}
