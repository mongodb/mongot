package com.xgen.mongot.index.lucene.document.single;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.bson.BsonValue;

/**
 * An AbstractDocumentWrapper is a container that contains information used for creating indexable
 * fields which are inserted into a Lucene Document.
 *
 * <p>When making choices about if a value type should be deserialized, it is not important if a
 * field is embedded or not - the configuring FieldDefinition and if the field is multi-valued (part
 * of an array or an array of documents) is the only information needed to make that determination.
 *
 * <p>Grouping the embedded root path of that document lets {@link
 * com.xgen.mongot.index.ingestion.handlers.DocumentHandler}s and {@link
 * com.xgen.mongot.index.ingestion.handlers.FieldValueHandler}s operate without knowledge of whether
 * the Lucene document they are building is part of an embedded document or not, and gives them an
 * easy way to delegate that information to places that create indexable fields and insert them into
 * documents.
 */
public abstract class AbstractDocumentWrapper {

  /**
   * A null-valued field is indexed with {@link SortedDocValuesField}, and Lucene throws an
   * exception while indexing if more than one {@link SortedDocValuesField} exists at the same path
   * in a Lucene document. Additionally, for querying purposes, mongot does not need to know if a
   * field contains multiple null values, but rather the existence of any null value.
   *
   * <p>This is used to keep track of the null fields in the Lucene document to avoid the exception
   * if the field contains multiple null values.
   */
  private final Set<String> nullFieldsIndexed;

  /**
   * Lucene throws an exception if there are multiple vectors nested within one vector field (nested
   * embeddings).
   *
   * <p>This is used to store the check field names of a vector field when it is indexed to avoid
   * the exception that lucene raises when there are multiple vectors within a vector field.
   */
  private final Set<String> vectorFieldsIndexed;

  /**
   * A numeric or date field indexed with {@link org.apache.lucene.document.NumericDocValuesField}
   * throws an exception if the same field name appears more than once in a Lucene document, because
   * NumericDocValuesField only allows one value per field. This can happen when a BSON document
   * contains duplicate keys at the same path.
   *
   * <p>This tracks which numeric DocValues fields have been indexed to skip duplicates and avoid
   * the exception.
   */
  private final Set<String> numericDocValuesFieldsIndexed;

  /**
   * This is used to store the check field names of a vector field when it is invalid for indexing
   * to avoid indexing the following vectors under the same vector field.
   */
  private final Set<String> vectorFieldsInvalidForIndexing;

  public final Document luceneDocument;
  public final IndexCapabilities indexCapabilities;
  public final IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater;

  AbstractDocumentWrapper(
      Document luceneDocument,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    this.luceneDocument = luceneDocument;
    this.indexCapabilities = indexCapabilities;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
    this.nullFieldsIndexed = new HashSet<>();
    this.vectorFieldsIndexed = new HashSet<>();
    this.vectorFieldsInvalidForIndexing = new HashSet<>();
    this.numericDocValuesFieldsIndexed = new HashSet<>();
  }

  /**
   * Returns the embedded root of the document wrapper.
   *
   * @return the embedded root of the document wrapper
   */
  Optional<FieldPath> getEmbeddedRoot() {
    return Optional.empty();
  }

  /**
   * Returns the index capabilities of the document wrapper.
   *
   * @return the index capabilities of the document wrapper
   */
  IndexCapabilities getIndexCapabilities() {
    return this.indexCapabilities;
  }

  /**
   * Adds the indexable field to the Lucene document.
   *
   * @param f - the indexable field to add
   */
  void put(IndexableField f) {
    this.luceneDocument.add(f);
  }

  /**
   * Adds a null field to the Lucene document if it is not already present.
   *
   * <p>Null fields are indexed with {@link SortedDocValuesField}, and Lucene throws an exception
   * while indexing if more than one {@link SortedDocValuesField} exists at the same path in a
   * Lucene document. This method prevents duplicate null fields by tracking indexed null fields.
   *
   * @param f - the null field to add
   * @param checkFieldName - the field name to use for duplicate tracking
   * @return true if the field was added, false if it was already present
   */
  boolean addNullFieldIfAbsent(IndexableField f, String checkFieldName) {
    if (this.isNullFieldIndexed(checkFieldName)) {
      return false;
    }
    this.put(f);
    this.addIndexedNullField(checkFieldName);
    return true;
  }

  /**
   * Adds a vector field to the Lucene document and marks the field name as indexed.
   *
   * @param f - the vector field to add
   * @param checkFieldName - the field name (from KNN_VECTOR) for duplicate and validity checking
   */
  void addVectorField(IndexableField f, String checkFieldName) {
    this.put(f);
    this.addIndexedVectorField(checkFieldName);
  }

  private boolean isNullFieldIndexed(String checkFieldName) {
    return this.nullFieldsIndexed.contains(checkFieldName);
  }

  private void addIndexedNullField(String checkFieldName) {
    this.nullFieldsIndexed.add(checkFieldName);
  }

  /**
   * Add the field name parameter to vectorFieldsIndexed hashset to keep track, and use this set to
   * ensure that at most there is only one vector value per Lucene field name.
   *
   * @param checkFieldName - the field name of the corresponding vector field
   */
  void addIndexedVectorField(String checkFieldName) {
    if (this.vectorFieldsIndexed.isEmpty()) {
      // Only increment for the first vector field discovered in a document.
      this.indexingMetricsUpdater.getVectorFieldsIndexed().increment();
    }
    this.vectorFieldsIndexed.add(checkFieldName);
  }

  /**
   * Adds the field name parameter to vectorFieldsInvalidForIndexing hashset to keep track of
   * invalid vector fields, and use this set to ensure that for a given vector field, if the first
   * vector is invalid for indexing, no subsequent or nested vectors under the same field are
   * indexed.
   *
   * @param checkFieldName - field name of the corresponding vector field
   */
  void markVectorFieldInvalid(String checkFieldName) {
    this.vectorFieldsInvalidForIndexing.add(checkFieldName);
  }

  /**
   * Checks if the given vector field name is valid for index.
   *
   * @param checkFieldName - field name of the corresponding vector field
   */
  boolean canIndexVectorField(String checkFieldName) {
    return !this.vectorFieldsInvalidForIndexing.contains(checkFieldName)
        && !this.vectorFieldsIndexed.contains(checkFieldName);
  }

  /**
   * Checks if a NumericDocValuesField can be indexed (has not already been added for this field
   * name). Returns true and marks the field as indexed if it can be added. Returns false if the
   * field has already been indexed.
   *
   * @param fieldName - the Lucene field name of the numeric DocValues field
   */
  boolean canIndexNumericDocValuesField(String fieldName) {
    return this.numericDocValuesFieldsIndexed.add(fieldName);
  }

  /**
   * Returns the index analyzer of the document wrapper.
   *
   * @return the index analyzer of the document wrapper
   */
  Optional<Analyzer> getIndexAnalyzer() {
    return Optional.empty();
  }

  /**
   * Returns the root ID of the document wrapper.
   *
   * @return the root ID of the document wrapper
   */
  BsonValue getRootId() {
    return LuceneDocumentIdEncoder.documentIdFromLuceneDocument(this.luceneDocument);
  }

  
}
