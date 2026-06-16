package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.sort.mixed.MqlMixedSort;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Test;

public class IndexSortUtilsTest {

  @Test
  public void testExtractFirstIndexSort_WithSort() throws IOException {
    Sort indexSort = new Sort(new SortField("date", SortField.Type.LONG));
    
    try (Directory directory = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig();
      config.setIndexSort(indexSort);
      
      try (IndexWriter writer = new IndexWriter(directory, config)) {
        Document doc = new Document();
        doc.add(new LongPoint("date", 20231201L));
        doc.add(new NumericDocValuesField("date", 20231201L));
        writer.addDocument(doc);
      }
      
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        Optional<Sort> result = IndexSortUtils.extractFirstIndexSort(reader);
        
        assertThat(result).isPresent();
        assertEquals(1, result.get().getSort().length);
        assertEquals("date", result.get().getSort()[0].getField());
        assertEquals(SortField.Type.LONG, result.get().getSort()[0].getType());
      }
    }
  }

  @Test
  public void testExtractFirstIndexSort_NoSort() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new LongPoint("date", 20231201L));
        doc.add(new NumericDocValuesField("date", 20231201L));
        writer.addDocument(doc);
      }
      
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        Optional<Sort> result = IndexSortUtils.extractFirstIndexSort(reader);
        assertFalse(result.isPresent());
      }
    }
  }

  @Test
  public void testUsesIndexSort_ExactMatch() {
    Sort querySort = new Sort(new SortField("date", SortField.Type.LONG));
    Sort indexSort = new Sort(new SortField("date", SortField.Type.LONG));

    assertThat(IndexSortUtils.usesIndexSort(querySort, indexSort)).isTrue();
  }

  @Test
  public void testUsesIndexSort_QueryIsPrefix() {
    Sort querySort = new Sort(new SortField("date", SortField.Type.LONG));
    Sort indexSort = new Sort(
        new SortField("date", SortField.Type.LONG),
        new SortField("score", SortField.Type.SCORE));

    assertThat(IndexSortUtils.usesIndexSort(querySort, indexSort)).isTrue();
  }

  @Test
  public void testUsesIndexSort_QueryLongerThanIndex() {
    Sort querySort = new Sort(
        new SortField("date", SortField.Type.LONG),
        new SortField("score", SortField.Type.SCORE));
    Sort indexSort = new Sort(new SortField("date", SortField.Type.LONG));

    assertFalse(IndexSortUtils.usesIndexSort(querySort, indexSort));
  }

  @Test
  public void testUsesIndexSort_DifferentFields() {
    Sort querySort = new Sort(new SortField("name", SortField.Type.STRING));
    Sort indexSort = new Sort(new SortField("date", SortField.Type.LONG));

    assertFalse(IndexSortUtils.usesIndexSort(querySort, indexSort));
  }

  @Test
  public void testUsesIndexSort_MqlLongSort_SameOrder() {
    MqlLongSort queryField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("score"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty(),
        true);
    MqlLongSort indexField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("score"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty(),
        true);

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertThat(IndexSortUtils.usesIndexSort(querySort, indexSort)).isTrue();
  }

  @Test
  public void testUsesIndexSort_MqlLongSort_DifferentOrder() {
    MqlLongSort queryField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("score"), UserFieldSortOptions.DEFAULT_DESC),
        true,
        Optional.empty(),
        true);
    MqlLongSort indexField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("score"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty(),
        true);

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertFalse(IndexSortUtils.usesIndexSort(querySort, indexSort));
  }

  @Test
  public void testUsesIndexSort_MqlDateSort_SameOrder() {
    MqlDateSort queryField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("date"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty(),
        true);
    MqlDateSort indexField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("date"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty(),
        true);

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertThat(IndexSortUtils.usesIndexSort(querySort, indexSort)).isTrue();
  }

  @Test
  public void testUsesIndexSort_MqlDateSort_DifferentOrder() {
    MqlDateSort queryField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("date"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty(),
        true);
    MqlDateSort indexField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("date"), UserFieldSortOptions.DEFAULT_DESC),
        true,
        Optional.empty(),
        true);

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertFalse(IndexSortUtils.usesIndexSort(querySort, indexSort));
  }

  @Test
  public void testUsesIndexSort_MixedMqlTypes_Incompatible() {
    MqlLongSort queryField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty(),
        true);
    MqlDateSort indexField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty(),
        true);

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertFalse(IndexSortUtils.usesIndexSort(querySort, indexSort));
  }

  // --- expandedSortAlignsWithIndexSort tests ---

  @Test
  public void expandedSortAligns_singleFieldAligned_returnsTrue() {
    List<MongotSortField> queryFields = List.of(userSortField("a"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_multipleFieldsAligned_returnsTrue() {
    List<MongotSortField> queryFields =
        List.of(userSortField("a"), userSortField("b"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"),
        nullnessSortField("b"),
        valueSortField("$type:int64V2/b"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_queryPrefixOfIndex_returnsTrue() {
    List<MongotSortField> queryFields = List.of(userSortField("a"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"),
        nullnessSortField("b"),
        valueSortField("$type:int64V2/b"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_misalignedValuePosition_returnsFalse() {
    List<MongotSortField> queryFields = List.of(userSortField("b"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"),
        nullnessSortField("b"),
        valueSortField("$type:int64V2/b"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isFalse();
  }

  @Test
  public void expandedSortAligns_expandedExceedsIndexLength_returnsFalse() {
    List<MongotSortField> queryFields =
        List.of(userSortField("a"), userSortField("b"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isFalse();
  }

  @Test
  public void expandedSortAligns_noNullnessInIndex_valueFieldStillAligned() {
    List<MongotSortField> queryFields = List.of(userSortField("a"));
    Sort indexSort = new Sort(valueSortField("$type:int64V2/a"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_mixedTypesWithTokenField_returnsTrue() {
    List<MongotSortField> queryFields =
        List.of(userSortField("num"), userSortField("name"));
    Sort indexSort = new Sort(
        nullnessSortField("num"),
        valueSortField("$type:int64V2/num"),
        valueSortField("$type:token/name"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_indexFieldWithoutTypePrefix_returnsFalse() {
    List<MongotSortField> queryFields = List.of(userSortField("a"));
    Sort indexSort = new Sort(valueSortField("plain_field_no_type"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isFalse();
  }

  private static MongotSortField userSortField(String path) {
    return new MongotSortField(
        FieldPath.newRoot(path), UserFieldSortOptions.DEFAULT_ASC);
  }

  private static SortedNumericSortField nullnessSortField(String path) {
    return new SortedNumericSortField(
        "$meta/nullness/" + path,
        SortField.Type.LONG,
        false,
        SortedNumericSelector.Type.MIN);
  }

  private static SortField valueSortField(String luceneFieldName) {
    return new SortedNumericSortField(
        luceneFieldName,
        SortField.Type.LONG,
        false,
        SortedNumericSelector.Type.MIN);
  }

  // --- CLOUDP-390552 serialization boundary regression tests ---
  //
  // Custom SortField subclasses (MqlLongSort, MqlSortedSetSortField, etc.) lose their concrete
  // type after a Lucene disk round-trip because Lucene deserializes them as the base
  // SortedNumericSortField / SortedSetSortField. Our Mql* sort field classes override equals()
  // to use instanceof instead of getClass(), so Lucene's canEarlyTerminateOnPrefix,
  // IndexWriter.validateIndexSort, and our usesIndexSort all work correctly across
  // the serialization boundary.

  @Test
  public void usesIndexSort_survivesSerializationBoundary_int64()
      throws IOException {
    MongotSortField scoreSortField =
        new MongotSortField(FieldPath.newRoot("score"), UserFieldSortOptions.DEFAULT_ASC);

    SortField valueField = LuceneSortFactory.createOptimizedSortField(
            scoreSortField,
            ImmutableSet.of(FieldName.TypeField.NUMBER_INT64_V2),
            Optional.empty(),
            Optional.empty(),
            SearchIndexCapabilities.CURRENT,
            true)
        .orElseThrow();

    Sort indexSort = new Sort(valueField);
    assertThat(valueField).isInstanceOf(MqlLongSort.class);

    try (Directory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig();
      config.setIndexSort(indexSort);
      String valueLuceneName =
          FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
              FieldPath.newRoot("score"), Optional.empty());

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField(valueLuceneName, 42L));
        writer.addDocument(doc);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Optional<Sort> deserializedSort = IndexSortUtils.extractFirstIndexSort(reader);
        assertThat(deserializedSort).isPresent();

        SortField[] deserialized = deserializedSort.get().getSort();
        assertThat(deserialized[0]).isNotInstanceOf(MqlLongSort.class);

        Sort freshQuerySort = new Sort(
            LuceneSortFactory.createOptimizedSortField(
                    scoreSortField,
                    ImmutableSet.of(FieldName.TypeField.NUMBER_INT64_V2),
                    Optional.empty(),
                    Optional.empty(),
                    SearchIndexCapabilities.CURRENT,
                    true)
                .orElseThrow());

        assertThat(IndexSortUtils.usesIndexSort(freshQuerySort, deserializedSort.get()))
            .isTrue();
      }
    }
  }

  @Test
  public void usesIndexSort_survivesSerializationBoundary_token()
      throws IOException {
    MongotSortField nameSortField =
        new MongotSortField(FieldPath.newRoot("name"), UserFieldSortOptions.DEFAULT_ASC);

    SortField valueField = LuceneSortFactory.createOptimizedSortField(
            nameSortField,
            ImmutableSet.of(FieldName.TypeField.TOKEN),
            Optional.empty(),
            Optional.empty(),
            SearchIndexCapabilities.CURRENT,
            true)
        .orElseThrow();

    Sort indexSort = new Sort(valueField);
    assertThat(valueField).isInstanceOf(MqlSortedSetSortField.class);

    try (Directory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig();
      config.setIndexSort(indexSort);
      String valueLuceneName =
          FieldName.TypeField.TOKEN.getLuceneFieldName(
              FieldPath.newRoot("name"), Optional.empty());

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        Document doc = new Document();
        doc.add(new org.apache.lucene.document.SortedSetDocValuesField(
            valueLuceneName, new org.apache.lucene.util.BytesRef("hello")));
        writer.addDocument(doc);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Optional<Sort> deserializedSort = IndexSortUtils.extractFirstIndexSort(reader);
        assertThat(deserializedSort).isPresent();

        SortField[] deserialized = deserializedSort.get().getSort();
        assertThat(deserialized[0]).isNotInstanceOf(MqlSortedSetSortField.class);

        Sort freshQuerySort = new Sort(
            LuceneSortFactory.createOptimizedSortField(
                    nameSortField,
                    ImmutableSet.of(FieldName.TypeField.TOKEN),
                    Optional.empty(),
                    Optional.empty(),
                    SearchIndexCapabilities.CURRENT,
                    true)
                .orElseThrow());

        assertThat(IndexSortUtils.usesIndexSort(freshQuerySort, deserializedSort.get()))
            .isTrue();
      }
    }
  }

  // --- Equality validation: ensure relaxed equals stays consistent with Lucene semantics ---

  @Test
  public void relaxedEquals_matchesLuceneSemantics_sortedNumeric() {
    SortedNumericSortField luceneA = new SortedNumericSortField(
        "field", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);
    SortedNumericSortField luceneB = new SortedNumericSortField(
        "field", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);

    MqlLongSort mql = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(
            FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC),
        true, Optional.empty(), true);

    assertThat(luceneA.equals(luceneB)).isTrue();

    SortedNumericSortField luceneMirror = new SortedNumericSortField(
        mql.getField(), mql.getNumericType(), mql.getReverse(), mql.getSelector());
    luceneMirror.setMissingValue(mql.getMissingValue());

    assertThat(mql.equals(luceneMirror)).isTrue();

    SortedNumericSortField wrongField = new SortedNumericSortField(
        "other", mql.getNumericType(), mql.getReverse(), mql.getSelector());
    wrongField.setMissingValue(mql.getMissingValue());
    assertThat(mql.equals(wrongField)).isFalse();
    assertThat(luceneMirror.equals(wrongField)).isFalse();

    SortedNumericSortField wrongType = new SortedNumericSortField(
        mql.getField(), SortField.Type.INT, mql.getReverse(), mql.getSelector());
    wrongType.setMissingValue(mql.getMissingValue());
    assertThat(mql.equals(wrongType)).isFalse();
    assertThat(luceneMirror.equals(wrongType)).isFalse();

    SortedNumericSortField wrongReverse = new SortedNumericSortField(
        mql.getField(), mql.getNumericType(), !mql.getReverse(), mql.getSelector());
    wrongReverse.setMissingValue(mql.getMissingValue());
    assertThat(mql.equals(wrongReverse)).isFalse();
    assertThat(luceneMirror.equals(wrongReverse)).isFalse();

    SortedNumericSortField wrongSelector = new SortedNumericSortField(
        mql.getField(), mql.getNumericType(), mql.getReverse(),
        SortedNumericSelector.Type.MAX);
    wrongSelector.setMissingValue(mql.getMissingValue());
    assertThat(mql.equals(wrongSelector)).isFalse();
    assertThat(luceneMirror.equals(wrongSelector)).isFalse();

    SortedNumericSortField wrongMissing = new SortedNumericSortField(
        mql.getField(), mql.getNumericType(), mql.getReverse(), mql.getSelector());
    wrongMissing.setMissingValue(Long.MAX_VALUE);
    assertThat(mql.equals(wrongMissing)).isFalse();
    assertThat(luceneMirror.equals(wrongMissing)).isFalse();
  }

  @Test
  public void relaxedEquals_matchesLuceneSemantics_sortedSet() {
    MongotSortField mongotSortField =
        new MongotSortField(
            FieldPath.newRoot("name"), UserFieldSortOptions.DEFAULT_ASC);
    MqlSortedSetSortField mql = MqlSortedSetSortField.stringSort(
        FieldName.TypeField.TOKEN, mongotSortField, true, Optional.empty(), true);

    SortedSetSortField luceneMirror = new SortedSetSortField(
        mql.getField(), mql.getReverse(), mql.getSelector());
    luceneMirror.setMissingValue(mql.getMissingValue());

    assertThat(mql.equals(luceneMirror)).isTrue();

    SortedSetSortField wrongField = new SortedSetSortField(
        "other", mql.getReverse(), mql.getSelector());
    wrongField.setMissingValue(mql.getMissingValue());
    assertThat(mql.equals(wrongField)).isFalse();
    assertThat(luceneMirror.equals(wrongField)).isFalse();

    SortedSetSortField wrongReverse = new SortedSetSortField(
        mql.getField(), !mql.getReverse(), mql.getSelector());
    wrongReverse.setMissingValue(mql.getMissingValue());
    assertThat(mql.equals(wrongReverse)).isFalse();
    assertThat(luceneMirror.equals(wrongReverse)).isFalse();

    SortedSetSortField wrongSelector = new SortedSetSortField(
        mql.getField(), mql.getReverse(), SortedSetSelector.Type.MAX);
    wrongSelector.setMissingValue(mql.getMissingValue());
    assertThat(mql.equals(wrongSelector)).isFalse();
    assertThat(luceneMirror.equals(wrongSelector)).isFalse();

    SortedSetSortField wrongMissing = new SortedSetSortField(
        mql.getField(), mql.getReverse(), mql.getSelector());
    wrongMissing.setMissingValue(SortField.STRING_LAST);
    if (!Objects.equals(mql.getMissingValue(), SortField.STRING_LAST)) {
      assertThat(mql.equals(wrongMissing)).isFalse();
      assertThat(luceneMirror.equals(wrongMissing)).isFalse();
    }
  }

  @Test
  public void relaxedEquals_MqlMixedSort_matchesDeserializedSortField() {
    MqlMixedSort mql = new MqlMixedSort(
        new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC),
        Optional.empty());

    SortField deserialized = new SortField("f", SortField.Type.CUSTOM, false);
    assertThat(mql.equals(deserialized)).isTrue();

    SortField wrongField = new SortField("g", SortField.Type.CUSTOM, false);
    assertThat(mql.equals(wrongField)).isFalse();

    SortField wrongReverse = new SortField("f", SortField.Type.CUSTOM, true);
    assertThat(mql.equals(wrongReverse)).isFalse();

    SortField wrongType = new SortField("f", SortField.Type.LONG, false);
    assertThat(mql.equals(wrongType)).isFalse();
  }

  @Test
  public void expandedSortAligns_int64PlusMqlMixedSort_returnsTrue() {
    List<MongotSortField> queryFields =
        List.of(userSortField("dateField"), userSortField("mixedField"));
    Sort indexSort = new Sort(
        nullnessSortField("dateField"),
        valueSortField("$type:int64V2/dateField"),
        new SortField("mixedField", SortField.Type.CUSTOM, false));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_mqlMixedSortOnly_returnsTrue() {
    List<MongotSortField> queryFields = List.of(userSortField("mixedField"));
    Sort indexSort = new Sort(
        new SortField("mixedField", SortField.Type.CUSTOM, false));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_mqlMixedSortWrongField_returnsFalse() {
    List<MongotSortField> queryFields = List.of(userSortField("mixedField"));
    Sort indexSort = new Sort(
        new SortField("otherField", SortField.Type.CUSTOM, false));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isFalse();
  }

  @Test
  public void usesIndexSort_survivesSerializationBoundary_mqlMixedSort()
      throws IOException {
    MongotSortField mongotField =
        new MongotSortField(FieldPath.newRoot("f"), UserFieldSortOptions.DEFAULT_ASC);
    MqlMixedSort valueField = new MqlMixedSort(mongotField, Optional.empty());

    Sort indexSort = new Sort(valueField);

    try (Directory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig();
      config.setIndexSort(indexSort);
      String intLuceneName =
          FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
              FieldPath.newRoot("f"), Optional.empty());

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField(intLuceneName, 42L));
        writer.addDocument(doc);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Optional<Sort> deserializedSort = IndexSortUtils.extractFirstIndexSort(reader);
        assertThat(deserializedSort).isPresent();

        SortField[] deserialized = deserializedSort.get().getSort();
        assertThat(deserialized[0]).isNotInstanceOf(MqlMixedSort.class);

        Sort freshQuerySort = new Sort(
            new MqlMixedSort(mongotField, Optional.empty()));

        assertThat(IndexSortUtils.usesIndexSort(freshQuerySort, deserializedSort.get()))
            .isTrue();
      }
    }
  }
}
