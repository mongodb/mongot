package com.xgen.mongot.index;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.status.IndexStatus;
import java.util.Map;

/**
 * An IndexMetricValuesSupplier can be used to retrieve values for metrics associated with an index.
 * Some metric values may be expensive to compute, and care should be taken to not call them more
 * than is necessary for the specific use case.
 *
 * <p>There is no guarantee that the values returned by this interface are consistent with each
 * other
 */
public interface IndexMetricValuesSupplier {

  class MetricNames {
    public static final String INDEX_SIZE_BYTES = "indexSizeBytes";
    public static final String LARGEST_INDEX_FILE_SIZE_BYTES = "largestIndexFileSizeBytes";
    public static final String NUMBER_OF_FILES_IN_INDEX = "numFilesInIndex";
    public static final String REQUIRED_MEMORY = "requiredMemoryBytes";
    public static final String NUM_LUCENE_FIELDS = "numLuceneFields";
    public static final String NUM_LUCENE_DOCS = "numLuceneDocs";
    public static final String NUM_LUCENE_MAX_DOCS = "numLuceneMaxDocs";
    public static final String INDEX_STATUS_CODE = "indexStatusCode";
    public static final String MAX_STRING_FACET_CARDINALITY = "maxStringFacetCardinality";
  }

  /**
   * Computes and returns the index size in bytes.
   *
   * <p><b>Warning:</b> This method is expensive as it may trigger directory walks or other I/O
   * operations. For hot paths like query execution, use {@link #getCachedIndexSize()} instead.
   *
   * @return the computed index size in bytes
   */
  long computeIndexSize();

  /**
   * Returns the cached index size in bytes.
   *
   * <p>This method returns a pre-computed value that is updated during async metrics collection,
   * making it safe to call on hot paths like query execution. Unlike {@link #computeIndexSize()},
   * this method never triggers expensive operations like directory walks.
   *
   * @return the cached index size in bytes, or 0 if not yet computed
   */
  long getCachedIndexSize();

  long getLargestIndexFileSize();

  long getNumFilesInIndex();

  int getNumFields();

  /** Returns a mapping of the number of fields present in an index per {@link TypeField}. */
  Map<TypeField, Double> getNumFieldsPerDatatype();

  DocCounts getDocCounts();

  IndexStatus getIndexStatus();

  void close();

  /**
   * Calls getRequiredMemoryForVectorData on the associated IndexReader object.
   *
   * @return long value representing the required memory of a vector index in bytes
   */
  long getRequiredMemoryForVectorData();
}
