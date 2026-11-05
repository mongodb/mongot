package com.xgen.mongot.index.definition;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.Enums;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Input modality of a Search autoEmbed field.
 *
 * <p>The wire value is the constant name converted to {@code CaseFormat.LOWER_CAMEL}, the same
 * conversion the index-BSON parser applies through {@code asCamelCase()}, so {@link #TEXT} is
 * spelled {@code "text"} and the two agree by construction. Parsing matches exactly: a value such
 * as {@code "TEXT"} is rejected while the definition is built, as with {@code similarity}.
 */
public enum Modality {
  TEXT;

  // Wire spelling → enum, derived with the same CaseFormat.LOWER_CAMEL conversion as the
  // index-BSON parser (enumField(...).asCamelCase()), so the two agree by construction.
  private static final ImmutableMap<String, Modality> NAME_TO_MODALITY =
      Arrays.stream(values())
          .collect(
              ImmutableMap.toImmutableMap(
                  modality -> Enums.convertNameTo(CaseFormat.LOWER_CAMEL, modality),
                  Function.identity()));

  /** Wire value, for example {@code text}. */
  public String value() {
    return Enums.convertNameTo(CaseFormat.LOWER_CAMEL, this);
  }

  /** Resolves that wire value, rejecting any other spelling. */
  public static Modality fromValue(String value) {
    Modality modality = NAME_TO_MODALITY.get(value);
    if (modality == null) {
      throw new IllegalArgumentException(
          String.format(
              "modality '%s' must be one of [%s]",
              value, String.join(", ", NAME_TO_MODALITY.keySet())));
    }
    return modality;
  }
}
