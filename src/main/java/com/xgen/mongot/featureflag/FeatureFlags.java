package com.xgen.mongot.featureflag;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.EnumMap;
import java.util.Objects;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;

/**
 * Immutable configuration of feature flags for Mongot.
 *
 * <p>A {@code FeatureFlags} represents the enabled/disabled state of all features in the system. It
 * is immutable and can be serialized to/from BSON for persistence and transmission.
 *
 * <p><b>Creating a FeatureFlags:</b>
 *
 * <ul>
 *   <li>{@link #getDefault()} - Returns the default configuration with all features in their
 *       default states
 *   <li>{@link #withDefaults()} - Returns a builder initialized with default states, allowing
 *       selective feature toggling
 *   <li>{@link #withQueryFeaturesEnabled()} - Returns a pre-configured set with query-related
 *       features enabled
 * </ul>
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * // Use default configuration
 * FeatureFlags config = FeatureFlags.getDefault();
 *
 * // Check if a feature is enabled
 * if (config.isEnabled(Feature.FACETING_OVER_TOKEN_FIELDS)) {
 *   // Use the feature
 * }
 *
 * // Create a custom configuration
 * FeatureFlags customConfig = FeatureFlags.withDefaults()
 *     .enable(Feature.FACETING_OVER_TOKEN_FIELDS)
 *     .enable(Feature.NEW_EMBEDDED_SEARCH_CAPABILITIES)
 *     .disable(Feature.STALE_STATE_TRANSITION)
 *     .build();
 *
 * // Serialize to BSON
 * BsonDocument doc = customConfig.toBson();
 *
 * // Deserialize from BSON
 * FeatureFlags restored = FeatureFlags.fromBson(parser);
 * }</pre>
 *
 * <p><b>Thread Safety:</b> {@code FeatureFlags} is immutable and thread-safe.
 *
 * @see Feature
 * @see FeatureFlags.Builder
 */
public class FeatureFlags implements DocumentEncodable {
  private static final FeatureFlags DEFAULT_INSTANCE = withDefaults().build();
  private final EnumMap<Feature, Feature.State> featureStates;

  /**
   * Constructs a FeatureFlags with the given feature states.
   *
   * <p>This constructor is private; use {@link #getDefault()}, or {@link #withDefaults()} to create
   * instances.
   *
   * @param featureStates the map of feature states
   */
  private FeatureFlags(EnumMap<Feature, Feature.State> featureStates) {
    this.featureStates = featureStates;
  }

  /**
   * Checks if the given feature is enabled in this configuration.
   *
   * @param feature the feature to check
   * @return {@code true} if the feature is enabled, {@code false} otherwise
   */
  public boolean isEnabled(Feature feature) {
    return this.featureStates.get(feature) == Feature.State.ENABLED;
  }

  /**
   * Deserializes a FeatureFlags from BSON using the provided parser.
   *
   * <p>This method reads all feature flags from the BSON document. For each feature, if the field
   * is present in the document, its value is used; otherwise, the feature's default state is used.
   *
   * @param parser the BSON parser to read from
   * @return a new FeatureFlags with the deserialized feature states
   * @throws BsonParseException if the BSON document is invalid
   */
  public static FeatureFlags fromBson(DocumentParser parser) throws BsonParseException {
    var builder = FeatureFlags.withDefaults();
    for (Feature feature : Feature.values()) {
      Field.WithDefault<Boolean> field =
          Field.builder(feature.getName())
              .booleanField()
              .optional()
              .withDefault(feature.getDefaultState() == Feature.State.ENABLED);
      boolean isEnabled = parser.getField(field).unwrap();
      if (isEnabled) {
        builder.enable(feature);
      } else {
        builder.disable(feature);
      }
    }
    return builder.build();
  }

  /**
   * Serializes this FeatureFlags to a BSON document.
   *
   * <p>Each feature is serialized as a boolean field using the feature's name.
   *
   * @return a BSON document representing this FeatureFlags
   */
  @Override
  public BsonDocument toBson() {
    var doc = new BsonDocument();
    for (Feature feature : Feature.values()) {
      doc.append(feature.getName(), new BsonBoolean(isEnabled(feature)));
    }
    return doc;
  }

  /**
   * Compares this FeatureFlags with another object for equality.
   *
   * <p>Two FeatureFlags are equal if they have the same enabled/disabled state for all features.
   *
   * @param o the object to compare with
   * @return {@code true} if the objects are equal, {@code false} otherwise
   */
  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FeatureFlags that = (FeatureFlags) o;
    return Objects.equals(this.featureStates, that.featureStates);
  }

  /**
   * Returns the hash code for this FeatureFlags.
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(this.featureStates);
  }

  /**
   * Returns a string representation of this FeatureFlags in JSON format.
   *
   * @return a JSON string representation
   */
  @Override
  public String toString() {
    return toBson().toJson();
  }

  /**
   * Returns the default FeatureFlags with all features in their default states.
   *
   * <p>This is a cached singleton instance that is reused across all calls.
   *
   * @return the default FeatureFlags
   */
  public static FeatureFlags getDefault() {
    return DEFAULT_INSTANCE;
  }

  /**
   * Returns a new builder initialized with all features in their default states.
   *
   * @return a new Builder
   */
  public static Builder withDefaults() {
    return new Builder();
  }

  /**
   * Builder for creating custom FeatureFlags configurations.
   *
   * <p>The builder is initialized with all features in their default states. Use {@link #enable}
   * and {@link #disable} to customize the configuration, then call {@link #build()} to create the
   * immutable FeatureFlags.
   *
   * <p>Example:
   *
   * <pre>{@code
   * FeatureFlags config = FeatureFlags.withDefaults()
   *     .enable(Feature.FACETING_OVER_TOKEN_FIELDS)
   *     .disable(Feature.STALE_STATE_TRANSITION)
   *     .build();
   * }</pre>
   */
  public static class Builder {
    private final EnumMap<Feature, Feature.State> featureStates = new EnumMap<>(Feature.class);

    /** Constructs a new Builder initialized with all features in their default states. */
    private Builder() {
      for (Feature feature : Feature.values()) {
        this.featureStates.put(feature, feature.getDefaultState());
      }
    }

    /**
     * Enables the given feature in this builder.
     *
     * @param feature the feature to enable
     * @return this builder for method chaining
     */
    public Builder enable(Feature feature) {
      this.featureStates.put(feature, Feature.State.ENABLED);
      return this;
    }

    /**
     * Disables the given feature in this builder.
     *
     * @param feature the feature to disable
     * @return this builder for method chaining
     */
    public Builder disable(Feature feature) {
      this.featureStates.put(feature, Feature.State.DISABLED);
      return this;
    }

    /**
     * Builds and returns an immutable FeatureFlags with the current configuration.
     *
     * @return a new FeatureFlags with the configured feature states
     */
    public FeatureFlags build() {
      return new FeatureFlags(this.featureStates);
    }
  }

  /**
   * Returns a pre-configured FeatureFlags with query-related features enabled.
   *
   * <p>This convenience method enables the following features:
   *
   * <ul>
   *   <li>{@link Feature#FACETING_OVER_TOKEN_FIELDS}
   *   <li>{@link Feature#NEW_EMBEDDED_SEARCH_CAPABILITIES}
   *   <li>{@link Feature#ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC}
   *   <li>{@link Feature#INDEX_FEATURE_VERSION_FOUR}
   *   <li>{@link Feature#SORTED_INDEX}
   * </ul>
   *
   * @return a FeatureFlags with query features enabled
   */
  public static FeatureFlags withQueryFeaturesEnabled() {
    return FeatureFlags.withDefaults()
        .enable(Feature.FACETING_OVER_TOKEN_FIELDS)
        .enable(Feature.NEW_EMBEDDED_SEARCH_CAPABILITIES)
        .enable(Feature.ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC)
        .enable(Feature.INDEX_FEATURE_VERSION_FOUR)
        .enable(Feature.SORTED_INDEX)
        .build();
  }
}
