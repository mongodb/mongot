package com.xgen.mongot.embedding.utils;

/**
 * Why an auto-embed index did a full document re-index instead of a partial-embed {@code $set}.
 * Most values come from {@link AutoEmbeddingDocumentUtils#computePartialEmbedChangedTexts}; {@link
 * #REPLACE_EVENT} and {@link #FILTER_EXTRACTION_FAILED} are recorded at the change-stream routing
 * layer. Exported as the snake_case {@code reason} tag on the {@code partialEmbedFallback} counter.
 */
public enum PartialEmbedFallbackReason {
  /** The update description was unavailable, so we can't reason about what changed. */
  NULL_UPDATE_DESCRIPTION,
  /** The event carried no full document, so embed-field text can't be extracted. */
  NULL_FULL_DOCUMENT,
  /** No update-description channel reported any change. */
  NO_CHANNELS_REPORTED,
  /** The field mapping doesn't enumerate every indexed path (non-vector mappings). */
  UNSUPPORTED_FIELD_MAPPING,
  /** A removed or truncated field touched a declared indexed path. */
  DESTRUCTIVE_INDEXED_TOUCH,
  /** An updated field is a strict ancestor of a declared indexed path. */
  ANCESTOR_OVERWRITE,
  /** A changed embed path overlaps another embed declaration. */
  OVERLAPPING_EMBEDS,
  /** An updated field touches an embed path indirectly (ancestor/descendant/array-indexed). */
  INDIRECT_EMBED_TOUCH,
  /** A changed embed source path traverses an array in the source document. */
  ARRAY_TRAVERSAL,
  /** Reading embed-field text out of the source document failed. */
  TEXT_EXTRACTION_FAILED,
  /** A changed embed path has no non-empty new text; a {@code $set} can't clear the MV vector. */
  EMPTY_NEW_VALUE,
  /** A REPLACE operation: a full document replacement, recorded at the routing layer. */
  REPLACE_EVENT,
  /** Filter-field extraction threw at the routing layer; falls back to a full replace. */
  FILTER_EXTRACTION_FAILED
}
