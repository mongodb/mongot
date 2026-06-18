package com.xgen.mongot.embedding.utils;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.FieldPath;
import java.util.Objects;
import java.util.Optional;

/**
 * Outcome of {@link AutoEmbeddingDocumentUtils#computePartialEmbedChangedTexts}: either the
 * partial-embed-safe {@code embed path → text} map to re-embed, or the {@link
 * PartialEmbedFallbackReason} for a full-replacement fallback. Exactly one is present.
 */
public record PartialEmbedClassification(
    Optional<ImmutableMap<FieldPath, String>> changedTexts,
    Optional<PartialEmbedFallbackReason> fallbackReason) {

  public PartialEmbedClassification {
    Objects.requireNonNull(changedTexts, "changedTexts");
    Objects.requireNonNull(fallbackReason, "fallbackReason");
    Verify.verify(
        changedTexts.isPresent() ^ fallbackReason.isPresent(),
        "exactly one of changedTexts / fallbackReason must be present");
  }

  static PartialEmbedClassification applied(ImmutableMap<FieldPath, String> changedTexts) {
    return new PartialEmbedClassification(Optional.of(changedTexts), Optional.empty());
  }

  static PartialEmbedClassification fellBack(PartialEmbedFallbackReason reason) {
    return new PartialEmbedClassification(Optional.empty(), Optional.of(reason));
  }
}
