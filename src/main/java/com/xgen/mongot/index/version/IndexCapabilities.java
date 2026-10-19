package com.xgen.mongot.index.version;

import com.google.errorprone.annotations.DoNotMock;

/**
 * An instance of this class defines which optional capabilities are supported for a given index.
 * $vectorSearch and $search indexes may differ in how they index certain types. Moreover, newer
 * $search indexes may support certain optional capabilities depending on when they were created
 * (determined by a combination of IndexFormatVersion and IndexFeatureVersion).
 *
 * <p>This class serves two purposes:
 *
 * <ol>
 *   <li>QueryFactory methods should provide stable method signatures. A reindex or IFV bump should
 *       not entail large cleanup refactorings
 *   <li>QueryFactories should not care if they are operating on search or vector search indexes.
 *       They should only be concerned with whether point/docvalue columns are available.
 * </ol>
 *
 * <p>The set of methods in this interface is expected to shrink after fleet-wide reindexing, and
 * then gradually expand again as we release new features.
 */
@DoNotMock
public interface IndexCapabilities {

  /**
   * Checks whether the version number of this mongot supports indexing doc values for ObjectIDs and
   * Booleans, which is used for sort, equality, and range queries. Specifically:
   *
   * <ul>
   *   <li>Indexing ObjectID typed fields as SortedSetDocValues in Lucene.
   *   <li>Indexing Boolean typed fields as SortedSetDocValues in Lucene.
   * </ul>
   */
  boolean supportsObjectIdAndBooleanDocValues();

  /**
   * Checks whether the version number of this mongot supports indexing
   * FieldName.MetaField.FIELD_NAMES in Lucene to support $exists queries
   */
  boolean supportsFieldExistsQuery();

  /**
   * True if {@code MetaField.ID} is indexed as SortedSetDocValues in Lucene.
   *
   * <p>Note: This is not the same as supporting sort on ObjectID, since _id can be any value. Also,
   * MetaField.ID does not follow MQL sort semantics. Instead, this value can only be used as a
   * tiebreaker in sort, which is valid because MQL explicitly states that tiebreaking behavior is
   * undefined.
   */
  boolean isMetaIdSortable();

  /**
   * True if this index supports TypeField.INT64_V2, TypeField.DOUBLE_V2, TypeField.DATE_V2 in
   * embedded documents.
   */
  boolean supportsEmbeddedNumericAndDateV2();

  /**
   * True if the autocomplete field is indexed with reduced Lucene IndexOptions ({@code
   * DOCS_AND_FREQS}) that omit the positions and offsets never consulted by the autocomplete query
   * path. Older indexes index it with {@code DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}.
   *
   * <p>Lucene forbids changing a field's IndexOptions within an existing index (it throws {@code
   * IllegalArgumentException} on the first inconsistent write), so this can only differ for indexes
   * (re)built at the newer feature version. Highlighting of an autocomplete-only field falls back
   * from the postings offset source to the UnifiedHighlighter's re-analysis offset source when the
   * offsets are absent.
   */
  boolean usesReducedAutocompleteIndexOptions();
}
