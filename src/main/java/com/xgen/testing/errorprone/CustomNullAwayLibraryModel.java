package com.xgen.testing.errorprone;

import static com.uber.nullaway.LibraryModels.MethodRef.methodRef;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.uber.nullaway.LibraryModels;

/**
 * This class is loaded by NullAway during compilation and provides a set of known nullable methods
 * in unannotated libraries.
 */
@AutoService(LibraryModels.class)
public class CustomNullAwayLibraryModel implements LibraryModels {

  @Override
  public ImmutableSetMultimap<MethodRef, Integer> failIfNullParameters() {
    return ImmutableSetMultimap.of();
  }

  @Override
  public ImmutableSetMultimap<MethodRef, Integer> explicitlyNullableParameters() {
    return ImmutableSetMultimap.of();
  }

  @Override
  public ImmutableSetMultimap<MethodRef, Integer> nonNullParameters() {
    return ImmutableSetMultimap.of();
  }

  @Override
  public ImmutableSetMultimap<MethodRef, Integer> nullImpliesTrueParameters() {
    return ImmutableSetMultimap.of();
  }

  @Override
  public ImmutableSetMultimap<MethodRef, Integer> nullImpliesFalseParameters() {
    return ImmutableSetMultimap.of();
  }

  @Override
  public ImmutableSetMultimap<MethodRef, Integer> nullImpliesNullParameters() {
    return ImmutableSetMultimap.of();
  }

  @Override
  public ImmutableSet<MethodRef> nullableReturns() {
    return NULLABLE_RETURNS;
  }

  private static final ImmutableSet<MethodRef> NULLABLE_RETURNS =
      ImmutableSet.of(
          // --- Explicit Lucene LeafReader doc-values APIs (no regex)
          methodRef("org.apache.lucene.index.LeafReader", "getNumericDocValues(java.lang.String)"),
          methodRef("org.apache.lucene.index.LeafReader", "getBinaryDocValues(java.lang.String)"),
          methodRef("org.apache.lucene.index.LeafReader", "getSortedDocValues(java.lang.String)"),
          methodRef(
              "org.apache.lucene.index.LeafReader", "getSortedNumericDocValues(java.lang.String)"),
          methodRef(
              "org.apache.lucene.index.LeafReader", "getSortedSetDocValues(java.lang.String)"),
          methodRef("org.apache.lucene.index.LeafReader", "getNormValues(java.lang.String)"),
          methodRef("org.apache.lucene.index.LeafReader", "getPointValues(java.lang.String)"),
          methodRef("org.apache.lucene.index.LeafReader", "getTermVectors(int)"),

          // --- Lucene Vector API (Lucene 9.11)
          methodRef(
              "org.apache.lucene.index.CodecReader", "getFloatVectorValues(java.lang.String)"),
          methodRef("org.apache.lucene.index.CodecReader", "getByteVectorValues(java.lang.String)"),

          // --- Other Lucene Reader APIs
          methodRef(
              "org.apache.lucene.index.DirectoryReader",
              "openIfChanged(org.apache.lucene.index.DirectoryReader)"),
          methodRef(
              "org.apache.lucene.index.DirectoryReader",
              "openIfChanged(org.apache.lucene.index.IndexReader,org.apache.lucene.index.CodecReader.Factory[])"),
          methodRef("org.apache.lucene.index.IndexReader", "getLiveDocs()"),

          // --- Lucene search classes
          methodRef("org.apache.lucene.search.TwoPhaseIterator", "twoPhaseIterator()"),
          methodRef("org.apache.lucene.search.FieldComparator", "value(int)"),

          // --- Lucene Weight nullable methods
          methodRef(
              "org.apache.lucene.search.Weight",
              "scorer(org.apache.lucene.index.LeafReaderContext)"),
          methodRef(
              "org.apache.lucene.search.Weight",
              "bulkScorer(org.apache.lucene.index.LeafReaderContext)"),
          methodRef(
              "org.apache.lucene.search.Weight",
              "scorerSupplier(org.apache.lucene.index.LeafReaderContext)"),

          // --- MongoDB Java client nullable methods
          methodRef(
              "com.mongodb.internal.operation.AggregateResponseBatchCursor", "getOperationTime()"),
          methodRef(
              "com.mongodb.internal.operation.AggregateResponseBatchCursor",
              "getPostBatchResumeToken()"),
          methodRef("com.mongodb.client.ChangeStreamBatchCursor", "getResumeToken()"),
          methodRef("com.mongodb.client.FindIterable", "first()"),
          methodRef("com.mongodb.client.MongoCollection", "findOneAndDelete()"),
          methodRef(
              "com.mongodb.client.MongoCollection",
              "findOneAndDelete(com.mongodb.client.model.Filters)"),
          methodRef(
              "com.mongodb.client.MongoCollection",
              "findOneAndDelete(com.mongodb.client.model.Filters,"
                  + " com.mongodb.client.model.FindOneAndDeleteOptions)"),
          methodRef(
              "com.mongodb.client.MongoCollection",
              "findOneAndReplace(com.mongodb.client.model.Filters, java.lang.Object)"),
          methodRef(
              "com.mongodb.client.MongoCollection",
              "findOneAndReplace(com.mongodb.client.model.Filters, java.lang.Object,"
                  + " com.mongodb.client.model.FindOneAndReplaceOptions)"),
          methodRef(
              "com.mongodb.client.MongoCollection",
              "findOneAndUpdate(com.mongodb.client.model.Filters,"
                  + " com.mongodb.client.model.Updates)"),
          methodRef(
              "com.mongodb.client.MongoCollection",
              "findOneAndUpdate(com.mongodb.client.model.Filters, com.mongodb.client.model.Updates,"
                  + " com.mongodb.client.model.FindOneAndUpdateOptions)"));

  @Override
  public ImmutableSet<MethodRef> nonNullReturns() {
    return ImmutableSet.of();
  }

  @Override
  public ImmutableSetMultimap<MethodRef, Integer> castToNonNullMethods() {
    return ImmutableSetMultimap.of();
  }
}
