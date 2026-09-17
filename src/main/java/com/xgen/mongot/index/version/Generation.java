package com.xgen.mongot.index.version;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public class Generation implements DocumentEncodable {
  static class Fields {
    static final Field.Required<Integer> USER_VERSION =
        Field.builder("userVersion").intField().mustBeNonNegative().required();
    static final Field.Required<Integer> FORMAT_VERSION =
        Field.builder("formatVersion").intField().mustBePositive().required();
    static final Field.WithDefault<Integer> ATTEMPT_NUMBER =
        Field.builder("attemptNumber").intField().mustBeNonNegative().optional().withDefault(0);
    static final Field.WithDefault<IndexEngine> ENGINE =
        Field.builder("engine")
            .enumField(IndexEngine.class)
            .asCamelCase()
            .optional()
            .withDefault(IndexEngine.LUCENE);
  }

  /** Generation assumed to all indexes defined before the introduction of no downtime indexing. */
  public static final Generation FIRST =
      new Generation(UserIndexVersion.FIRST, IndexFormatVersion.FIVE);

  /** Generation of Lucene indexes created now. */
  public static final Generation CURRENT =
      new Generation(UserIndexVersion.FIRST, IndexFormatVersion.CURRENT);

  public final UserIndexVersion userIndexVersion;
  public final FormatVersion formatVersion;
  public final int attemptNumber;

  public Generation(
      UserIndexVersion userIndexVersion, FormatVersion formatVersion, int attemptNumber) {
    this.userIndexVersion = userIndexVersion;
    this.formatVersion = formatVersion;
    this.attemptNumber = attemptNumber;
  }

  public Generation(UserIndexVersion userIndexVersion, FormatVersion formatVersion) {
    this(userIndexVersion, formatVersion, 0);
  }

  /** Generation an index created now starts at. */
  public static Generation current(IndexEngine engine, UserIndexVersion userIndexVersion) {
    return new Generation(userIndexVersion, FormatVersion.current(engine));
  }

  /** deserialize. */
  public static Generation fromBson(DocumentParser parser) throws BsonParseException {
    var user = new UserIndexVersion(parser.getField(Fields.USER_VERSION).unwrap());
    int formatVersion = parser.getField(Fields.FORMAT_VERSION).unwrap();
    int attempt = parser.getField(Fields.ATTEMPT_NUMBER).unwrap();
    IndexEngine engine = parser.getField(Fields.ENGINE).unwrap();
    return new Generation(user, FormatVersion.of(engine, formatVersion), attempt);
  }

  // This method should only be used in tests.
  @VisibleForTesting
  public Generation incrementUser() {
    return new Generation(this.userIndexVersion.increment(), this.formatVersion);
  }

  public Generation nextAttempt() {
    return new Generation(this.userIndexVersion, this.formatVersion, this.attemptNumber + 1);
  }

  public GenerationId generationId(ObjectId indexId) {
    return new GenerationId(indexId, this);
  }

  /**
   * Lucene format version the index capabilities derive from. A vectorlite generation has none and
   * uses the current one.
   */
  public IndexFormatVersion capabilitiesFormatVersion() {
    return this.formatVersion instanceof IndexFormatVersion lucene
        ? lucene
        : IndexFormatVersion.CURRENT;
  }

  @Override
  public BsonDocument toBson() {
    var documentBuilder =
        BsonDocumentBuilder.builder()
            .field(Fields.USER_VERSION, this.userIndexVersion.versionNumber)
            .field(Fields.FORMAT_VERSION, this.formatVersion.versionNumber());

    // Only serialize the attempt number if it is greater than zero, to maintain downgrade
    // compatibility with versions of mongot which cannot parse this field.
    if (this.attemptNumber > 0) {
      documentBuilder.field(Fields.ATTEMPT_NUMBER, this.attemptNumber);
    }

    // Absent for Lucene, so their journal entries are unchanged.
    if (this.formatVersion.engine() != IndexEngine.LUCENE) {
      documentBuilder.field(Fields.ENGINE, this.formatVersion.engine());
    }

    return documentBuilder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Generation)) {
      return false;
    }
    Generation that = (Generation) o;
    return this.userIndexVersion.equals(that.userIndexVersion)
        && this.formatVersion.equals(that.formatVersion)
        && this.attemptNumber == that.attemptNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.userIndexVersion, this.formatVersion, this.attemptNumber);
  }

  @Override
  public String toString() {
    IndexEngine engine = this.formatVersion.engine();
    return String.format(
        "Generation{user=%d, format=%d, attempt=%d%s}",
        this.userIndexVersion.versionNumber,
        this.formatVersion.versionNumber(),
        this.attemptNumber,
        engine == IndexEngine.LUCENE ? "" : ", engine=" + engine);
  }
}
