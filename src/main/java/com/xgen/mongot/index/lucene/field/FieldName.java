package com.xgen.mongot.index.lucene.field;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.FieldPath;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.util.UnicodeUtil;

/**
 * A note on performance: this class is used in most performance-critical parts of the code (e.g.
 * BsonDocumentConverter). If any changes are needed, please check related benchmark results before
 * and after merging new code.
 *
 * <p>{@link FieldName} contains static sub-classes and enums that provide functionality related to
 * field names stored in Lucene for particular types of data.
 */
public class FieldName {

  /**
   * {@link StaticField} describes fields that are always the same; these fields are exactly the
   * same wherever they are used. Static fields may not be prefixed with embedded paths.
   */
  public enum StaticField {
    STORED_SOURCE("$storedSource"),
    FACET(FacetsConfig.DEFAULT_INDEX_FIELD_NAME);

    private final String fieldName;

    StaticField(String fieldName) {
      this.fieldName = fieldName;
    }

    public String getLuceneFieldName() {
      return this.fieldName;
    }

    public boolean isTypeOf(String luceneFieldName) {
      return this.fieldName.equals(luceneFieldName);
    }
  }

  /**
   * {@link MetaField} describes static fields that begin with "$meta". Meta fields may not be
   * prefixed with embedded paths.
   */
  public enum MetaField {
    EMBEDDED_PATH("embeddedPath"),
    EMBEDDED_ROOT("embeddedRoot"),
    FIELD_NAMES("fieldNames"),
    ID("_id"),
    PARENT_FIELD("parentField"),
    /** Internal meta field used to encode null/noData ordering for index sort. */
    NULLNESS("nullness"),
    CUSTOM_VECTOR_ENGINE_ID("customVectorEngineId");

    private static final String PREFIX = "$meta/";

    private final String fieldName;

    MetaField(String fieldType) {
      this.fieldName = PREFIX + fieldType;
    }

    public boolean isTypeOf(String luceneFieldName) {
      return this.fieldName.equals(luceneFieldName);
    }

    public String getLuceneFieldName() {
      return this.fieldName;
    }

    public static boolean isMetaField(String luceneField) {
      return luceneField.startsWith(PREFIX);
    }
  }

  /**
   * {@link TypeField} describes fields that begin with "$type". Type fields are prefixed by
   * embedded paths when an indexed field is a child of an embeddedDocuments field.
   */
  public enum TypeField {
    AUTOCOMPLETE("autocomplete"),
    /**
     * Indexes arrays and non-array fields as a StringField (See {@link
     * FieldValue#fromBoolean(boolean)})
     *
     * <p><b>Restrictions:</b>
     *
     * <ul>
     *   <li>May have docValues depending on IndexCapabilities
     * </ul>
     */
    BOOLEAN("boolean"),
    /** Indexes non-array fields as NumericDocValuesField and LongPoint */
    DATE("date"),
    /**
     * Indexes array and non-array fields as SortedNumericDocValuesField and LongPoint.
     *
     * <p><b>Restrictions:</b>
     *
     * <ul>
     *   <li>Not available in embeddedDocuments.
     * </ul>
     */
    DATE_V2("dateV2"),
    /** Indexes array fields as LongPoint */
    DATE_MULTIPLE("dateMultiple"),
    /** Indexes non-array fields as NumericDocValuesField and LongPoint */
    DATE_FACET("dateFacet"),
    GEO_POINT("geoPoint"),
    GEO_SHAPE("geoShape"),
    /** KNN Float Vector field (without quantization) */
    KNN_VECTOR("knnVector"),
    /** KNN Byte Vector field (without quantization) */
    KNN_BYTE("knnByte"),
    /** KNN Bit Vector field (without quantization) */
    KNN_BIT("knnBit"),
    /** KNN Float Vector field with scalar quantization to int7 */
    KNN_F32_Q7("knnF32Q7"),
    /** KNN Float Vector field with binary quantization */
    KNN_F32_Q1("knnF32Q1"),
    /**
     * Indexes array and non-array fields as StringField and SortedDocValuesField.
     *
     * <p>Note: This differs from other term fields since it uses SortedDocValuesField rather than
     * SortedSetDocValuesField
     */
    NULL("null"),
    /** Indexes non-array fields as NumericDocValuesField and LongPoint */
    NUMBER_DOUBLE("double"),
    /**
     * Indexes array and non-array fields as SortedNumericDocValuesField and LongPoint.
     *
     * <p><b>Restrictions:</b>
     *
     * <ul>
     *   <li>Not available in embeddedDocuments.
     *   <li>Encoding differs from NUMBER_DOUBLE. It is <b>NOT</b> a drop-in replacement
     *   <li>Ordering for NaN differs from NUMBER_DOUBLE. It cannot be used for range queries
     * </ul>
     */
    NUMBER_DOUBLE_V2("doubleV2"),
    /** Indexes array fields as LongPoint */
    NUMBER_DOUBLE_MULTIPLE("doubleMultiple"),
    /** Indexes NumericDocValuesField for scalar fields. */
    NUMBER_DOUBLE_FACET("doubleFacet"),
    /** Indexes non-array fields as NumericDocValuesField and LongPoint. */
    NUMBER_INT64("int64"),
    /**
     * Indexes array and non-array fields as SortedNumericDocValuesField and LongPoint.
     *
     * <p><b>Restrictions:</b>
     *
     * <ul>
     *   <li>Not available in embeddedDocuments.
     *   <li>Encoding differs from NUMBER_INT64. It is <b>NOT</b> a drop-in replacement
     *   <li>Ordering for NaN differs from NUMBER_INT64. It cannot be used for range queries
     * </ul>
     */
    NUMBER_INT64_V2("int64V2"),
    /** Indexes array fields as NumericDocValuesField and LongPoint */
    NUMBER_INT64_MULTIPLE("int64Multiple"),
    /** Indexes NumericDocValuesField for scalar fields. */
    NUMBER_INT64_FACET("int64Facet"),
    /**
     * Indexes an ObjectID as a 12 byte StringField with norms.
     *
     * <p><b>Restrictions:</b>
     *
     * <ul>
     *   <li>May have docValues depending on IndexCapabilities
     * </ul>
     */
    OBJECT_ID("objectId"),
    SORTABLE_DATE_BETA_V1("sortableDateBetaV1"),
    SORTABLE_NUMBER_BETA_V1("sortableNumberBetaV1"),
    SORTABLE_STRING_BETA_V1("sortableStringBetaV1"),
    STRING("string"),
    /**
     * Indexes array and non-array fields as a StringField and SortedSetDocValuesField with possible
     * case normalization but no tokenization.
     *
     * <p>Note: Value is truncated to LuceneConfig.MAX_TERM_CHAR_LENGTH chars
     */
    TOKEN("token"),
    /**
     * Indexes the result of {@link UUID#toString()} as StringField and SortedSetDocValuesField.
     *
     * <p>Note: Only applies to BsonBinary subtype=UUID_STANDARD
     */
    UUID("uuid");

    private static final String PREFIX = "$type:";

    private static final String DELIMITER = "/";

    private static final ImmutableMap<String, TypeField> PREFIX_TO_TYPE;

    static {
      PREFIX_TO_TYPE =
          Arrays.stream(TypeField.values())
              .collect(
                  ImmutableMap.toImmutableMap(
                      typeField -> typeField.fieldPrefix + DELIMITER, Function.identity()));
    }

    private final String fieldPrefix;

    TypeField(String fieldType) {
      this.fieldPrefix = PREFIX + fieldType;
    }

    public static Optional<TypeField> getTypeOf(String luceneField) {
      // Strip embedded prefix from the luceneField
      String strippedField = EmbeddedField.stripPrefix(luceneField);

      // Extract up to the first occurrence of DELIMITER for lookup
      int index = strippedField.indexOf(DELIMITER);
      if (index == -1 || index + DELIMITER.length() >= strippedField.length()) {
        // Return empty if no delimiter or if there's no content after the delimiter
        return Optional.empty();
      }

      // The substring is used to query the PREFIX_TO_TYPE map
      String fieldPrefixKey = strippedField.substring(0, index + DELIMITER.length());
      return Optional.ofNullable(PREFIX_TO_TYPE.get(fieldPrefixKey));
    }

    public String getLuceneFieldName(FieldPath field, Optional<FieldPath> embeddedRoot) {
      return EmbeddedField.prependPrefix(this.fieldPrefix + DELIMITER + field, embeddedRoot);
    }

    public boolean isTypeOf(String luceneField) {
      String luceneFieldNoEmbedded = EmbeddedField.stripPrefix(luceneField);
      return luceneFieldNoEmbedded.startsWith(this.fieldPrefix + DELIMITER);
    }

    public Predicate<String> isType() {
      return this::isTypeOf;
    }

    public String stripPrefix(String luceneField) {
      if (!isTypeOf(luceneField)) {
        throw new AssertionError(
            String.format("cannot strip %s field name prefix from %s field", this, luceneField));
      }

      return EmbeddedField.stripPrefix(luceneField)
          .substring(this.fieldPrefix.length() + DELIMITER.length());
    }
  }

  /**
   * {@link MultiField} describes string multi fields. Multi fields are different from Type fields
   * in that they are created from a {@link StringMultiFieldPath} instead of a {@code String} or
   * {@link FieldPath}. Multi fields are prefixed by embedded paths when an indexed field is a child
   * of an embeddedDocuments field.
   */
  public static class MultiField {
    private static final String PREFIX = "$multi/";
    private static final String DELIMITER = ".";

    public static String getLuceneFieldName(
        StringMultiFieldPath multiFieldPath, Optional<FieldPath> embeddedRoot) {
      return getLuceneFieldName(
          multiFieldPath.getFieldPath(), multiFieldPath.getMulti(), embeddedRoot);
    }

    public static String getLuceneFieldName(
        FieldPath fieldPath, String multiName, Optional<FieldPath> embeddedRoot) {
      return EmbeddedField.prependPrefix(PREFIX + fieldPath + DELIMITER + multiName, embeddedRoot);
    }

    public static boolean isTypeOf(String luceneField) {
      return EmbeddedField.stripPrefix(luceneField).startsWith(PREFIX);
    }

    public static Predicate<String> isType() {
      return MultiField::isTypeOf;
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static StringMultiFieldPath getFieldPath(String luceneField) {
      if (!isTypeOf(luceneField)) {
        throw new AssertionError(
            String.format("cannot strip multi field name prefix from %s field", luceneField));
      }

      // The characters after "$multi/" should be the path.
      String multiPath = EmbeddedField.stripPrefix(luceneField).substring(PREFIX.length());

      int lastDotIdx = multiPath.lastIndexOf(DELIMITER);
      if (lastDotIdx == -1) {
        throw new IllegalArgumentException("multi path did not contain a " + DELIMITER);
      }

      if (lastDotIdx == multiPath.length() - 1) {
        throw new IllegalArgumentException(
            "multi path did not have a multi name after the final " + DELIMITER);
      }

      FieldPath path = FieldPath.parse(multiPath.substring(0, lastDotIdx));
      String multiName = multiPath.substring(lastDotIdx + 1);
      return new StringMultiFieldPath(path, multiName);
    }
  }

  /**
   * {@link EmbeddedField} contains methods that allow other fields to create and understand field
   * names of fields that are part of an embedded document.
   *
   * <p>{@code EmbeddedField} is always used to wrap other fields, and does not contain means to
   * create new fields; it only groups associated logic related to embedded fields.
   */
  public static class EmbeddedField {
    private static final String PREFIX = "$embedded:";
    private static final String DELIMITER = "/";

    /**
     * {@code PREFIX.length()} is technically the number of UTF-16 code points in {@code PREFIX},
     * but all characters in {@code PREFIX} are representable with a single UTF-8 code unit, so this
     * length is also the number of UTF-8 code units of {@code PREFIX}.
     */
    private static final int PREFIX_UTF_8_CODE_UNIT_COUNT = PREFIX.length();

    public static boolean isTypeOf(String luceneFieldName) {
      return luceneFieldName.startsWith(PREFIX);
    }

    /**
     * Returns a predicate that evaluates whether a String field name is part of an embedded
     * document at some provided path. If the provided path is empty, check that the String field
     * name is not indexed as part of an embedded document.
     */
    public static Predicate<String> isAtEmbeddedRoot(Optional<FieldPath> embeddedRoot) {
      return fieldName ->
          embeddedRoot
              .map(FieldName.EmbeddedField::getPrefix)
              .map(fieldName::startsWith)
              .orElse(!isTypeOf(fieldName));
    }

    /**
     * Fields that are part of an embedded document have field names with a special prefix related
     * to the embedded document that they are a part of. Embedded field names are constructed by
     * prepending existing field names with a series of concatenated string values:
     *
     * <ol>
     *   <li>a special "{@code $embedded}" marker, followed by a "{@code :}".
     *   <li>the length of the path of the {@code embeddedDocuments} field, followed by a "{@code
     *       /}".
     *   <li>the path of the "{@code embeddedDocuments}" field that this field is indexed as a part
     *       of, followed by a "{@code /}".
     *   <li>the normal field encoding scheme for any particular field.
     * </ol>
     *
     * <p>The <a href="https://tinyurl.com/2f682wk2">Field Name Prefixes section of the embedded
     * spec</a> describe how field name prefixes are constructed and used in more detail.
     */
    @VisibleForTesting
    static String prependPrefix(String fieldName, Optional<FieldPath> maybeEmbeddedRoot) {
      return maybeEmbeddedRoot
          .map(EmbeddedField::getPrefix)
          .map(prefix -> prefix + fieldName)
          .orElse(fieldName);
    }

    private static String getPrefix(FieldPath embeddedRootPath) {
      String embeddedRoot = embeddedRootPath.toString();

      int utf8CodeUnits = UnicodeUtil.calcUTF16toUTF8Length(embeddedRoot, 0, embeddedRoot.length());

      return PREFIX + utf8CodeUnits + DELIMITER + embeddedRoot + DELIMITER;
    }

    /**
     * Fields that are part of an embedded document have field names with a special prefix related
     * to the embedded document that they are a part of. Embedded field name prefixes can be removed
     * from embedded fields by roughly the following process:
     *
     * <ul>
     *   <li>if the field name begins with "{@code $embedded}":
     *       <ol>
     *         <li>extract embeddedDocuments field length from characters
     *             <ul>
     *               <li>after "{@code $embedded}"
     *               <li>before the first forward slash character
     *             </ul>
     *         <li>extract embeddedDocuments field from character array of known length immediately
     *             following first forward slash
     *         <li>beginning of "normal" field name encoding follows the forward slash that follows
     *             the end of the embeddedDocuments field path
     *         <li>decode rest of field name as done before
     *       </ol>
     *   <li>else (field does not begin with "{@code $embedded}"):
     *       <ul>
     *         <li>decode field name as done before
     *       </ul>
     * </ul>
     *
     * <p>Length of a field is measured as the number of UTF-8 code units needed to represent a
     * string.
     *
     * <p>The <a href="https://tinyurl.com/mr7tkn84">Encoding User Input section of the embedded
     * spec</a> describe how field name prefixes are removed from embedded fields and used in more
     * detail.
     */
    @VisibleForTesting
    static String stripPrefix(String fieldName) {
      if (!fieldName.startsWith(PREFIX)) {
        return fieldName;
      }

      // Get offset of first '/' to determine where integer following '$embedded:' prefix ends.
      int firstDelimiterUtf8CodeUnitOffset =
          fieldName.indexOf(DELIMITER, PREFIX_UTF_8_CODE_UNIT_COUNT);

      // Decode integer representing number of UTF-8 code units (bytes) in the embedded root path.
      int embeddedRootUtf8CodeUnitCount =
          Integer.parseInt(
              fieldName, PREFIX_UTF_8_CODE_UNIT_COUNT, firstDelimiterUtf8CodeUnitOffset, 10);

      byte[] utf8CodeUnitBytes = fieldName.getBytes(StandardCharsets.UTF_8);

      // Add '2' to this value to account for two '/' delimiter characters.
      int regularFieldNameOffset =
          firstDelimiterUtf8CodeUnitOffset + embeddedRootUtf8CodeUnitCount + 2;

      return new String(
          Arrays.copyOfRange(utf8CodeUnitBytes, regularFieldNameOffset, utf8CodeUnitBytes.length),
          StandardCharsets.UTF_8);
    }

    private static String getEmbeddedRoot(String fieldName) {
      if (!fieldName.startsWith(PREFIX)) {
        return fieldName;
      }

      // Get offset of first '/' to determine where integer following '$embedded:' prefix ends.
      int firstDelimiterUtf8CodeUnitOffset =
          fieldName.indexOf(DELIMITER, PREFIX_UTF_8_CODE_UNIT_COUNT);

      // Decode integer representing number of UTF-8 code units (bytes) in the embedded root path.
      int embeddedRootUtf8CodeUnitCount =
          Integer.parseInt(
              fieldName, PREFIX_UTF_8_CODE_UNIT_COUNT, firstDelimiterUtf8CodeUnitOffset, 10);

      byte[] utf8CodeUnitBytes = fieldName.getBytes(StandardCharsets.UTF_8);

      // Add '1' to this value to account for a '/' delimiter character.
      int regularFieldNameOffset =
          firstDelimiterUtf8CodeUnitOffset + embeddedRootUtf8CodeUnitCount + 1;

      return new String(
          Arrays.copyOfRange(
              utf8CodeUnitBytes, firstDelimiterUtf8CodeUnitOffset + 1, regularFieldNameOffset),
          StandardCharsets.UTF_8);
    }
  }

  /** Returns the field name to be stored in lucene documents for the given StringPath. */
  public static String getLuceneFieldNameForStringPath(
      StringPath stringPath, Optional<FieldPath> embeddedRoot) {
    return switch (stringPath.getType()) {
      case FIELD ->
          TypeField.STRING.getLuceneFieldName(stringPath.asField().getValue(), embeddedRoot);
      case MULTI_FIELD -> MultiField.getLuceneFieldName(stringPath.asMultiField(), embeddedRoot);
    };
  }

  public static String stripAnyPrefixFromLuceneFieldName(String luceneFieldName) {
    if (EmbeddedField.isTypeOf(luceneFieldName)) {
      String withoutPrefix = EmbeddedField.stripPrefix(luceneFieldName);

      // Add 1 to count the `/` after the type prefix.
      return withoutPrefix.substring(withoutPrefix.indexOf('/') + 1);
    }

    return luceneFieldName.substring(luceneFieldName.indexOf('/') + 1);
  }

  public static String getEmbeddedRootPathOrThrow(String luceneFieldName) {
    if (EmbeddedField.isTypeOf(luceneFieldName)) {
      return EmbeddedField.getEmbeddedRoot(luceneFieldName);
    }
    throw new IllegalArgumentException(
        String.format("%s is not a embedded field path", luceneFieldName));
  }

  public static FieldPath getFieldPath(String luceneFieldName) {
    return FieldPath.parse(stripAnyPrefixFromLuceneFieldName(luceneFieldName));
  }

  /**
   * Get prefix of luceneFieldName before the field path begins, preserving the embedded root if
   * there is one. Ex. $embedded:6/field0/$multi/field0.multi0 -> $embedded:6/field0/$multi/
   *
   * <p>This method does NOT apply for {@link MetaField} and {@link StaticField}, but does support
   * {@link TypeField}, {@link MultiField}
   *
   * @param luceneFieldName field name for which to get prefix of
   * @return luceneFieldName prefix
   */
  public static String getPrefixFromLuceneFieldNameForTypeFieldOrMultiField(
      String luceneFieldName) {
    // $multi or $type will be the last markers before the field path
    if (MultiField.isTypeOf(luceneFieldName)) {
      String marker = MultiField.PREFIX;
      return luceneFieldName.substring(0, luceneFieldName.indexOf(marker) + marker.length());
    }

    String marker = TypeField.PREFIX;
    // Get index of marker and then get the index of '/' delimiter after, then add one to account
    // for delimiter
    return luceneFieldName.substring(
        0, luceneFieldName.indexOf('/', luceneFieldName.indexOf(marker) + marker.length()) + 1);
  }

  public record Components(
      Optional<FieldPath> embeddedRoot,
      FieldName.TypeField typeField,
      Optional<StringMultiFieldPath> multiFieldPath,
      FieldPath fieldPath) {}

  /**
   * Extracts the components of a Lucene field name, specifically for fields that are either
   * TypeFields or MultiFields, and can optionally be embedded. This method does not handle
   * StaticField or MetaField types.
   *
   * @param luceneFieldName The full Lucene field name, e.g., "$embedded:3/foo/$type:token/foo.bar"
   *     or "$multi/field0.multi0".
   * @return A {@link Components} object containing the extracted components.
   * @throws IllegalArgumentException if the field name does not conform to the expected format
   *     (i.e., not a TypeField or MultiField, or an invalid embedded structure).
   */
  public static Components extractFieldNameComponents(String luceneFieldName) {
    // 1. Check for Embedded Field first
    if (EmbeddedField.isTypeOf(luceneFieldName)) {
      try {
        return extractNonEmbeddedComponents(
            EmbeddedField.stripPrefix(luceneFieldName),
            Optional.of(FieldPath.parse(getEmbeddedRootPathOrThrow(luceneFieldName))));
      } catch (IllegalArgumentException e) {
        // Re-throw with more specific context for embedded fields
        throw new IllegalArgumentException(
            String.format(
                "Embedded field name does not contain a valid "
                    + "TypeField or MultiField suffix: %s. Original error: %s",
                luceneFieldName, e.getMessage()),
            e);
      }
    } else {
      // 2. If not an Embedded Field, check if it's a top-level TypeField or MultiField
      // We can directly call the helper method with an empty embeddedRoot.
      try {
        return extractNonEmbeddedComponents(luceneFieldName, Optional.empty());
      } catch (IllegalArgumentException e) {
        // Re-throw with more specific context for non-embedded fields
        throw new IllegalArgumentException(
            String.format(
                "The luceneFieldName is not a recognized top-level "
                    + "TypeField or MultiField format: %s. Original error: %s",
                luceneFieldName, e.getMessage()),
            e);
      }
    }
  }

  private static Components extractNonEmbeddedComponents(
      String fieldName, Optional<FieldPath> embeddedRoot) {

    FieldPath extractedFieldPath;

    if (TypeField.getTypeOf(fieldName).isPresent()) {
      TypeField typeField = TypeField.getTypeOf(fieldName).get();
      extractedFieldPath = FieldPath.parse(typeField.stripPrefix(fieldName));
      return new Components(embeddedRoot, typeField, Optional.empty(), extractedFieldPath);
    } else if (MultiField.isTypeOf(fieldName)) {
      StringMultiFieldPath multiFieldPath = MultiField.getFieldPath(fieldName);
      extractedFieldPath = multiFieldPath.getFieldPath();
      return new Components(
          embeddedRoot, TypeField.STRING, Optional.of(multiFieldPath), extractedFieldPath);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Field name does not contain a valid TypeField or MultiField component: %s",
              fieldName));
    }
  }

  public static String getNullnessFieldName(FieldPath path) {
    return MetaField.NULLNESS.getLuceneFieldName() + "/" + path;
  }
}
