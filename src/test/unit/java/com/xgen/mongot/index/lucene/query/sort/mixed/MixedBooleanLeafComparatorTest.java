package com.xgen.mongot.index.lucene.query.sort.mixed;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBoolean;
import org.bson.BsonNull;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      MixedBooleanLeafComparatorTest.SegmentWithTrueFalseTest.class,
      MixedBooleanLeafComparatorTest.SegmentWithOnlyTrueTest.class,
      MixedBooleanLeafComparatorTest.SegmentWithOnlyFalseTest.class
    })
public class MixedBooleanLeafComparatorTest {
  private static final String FIELD = "field";

  public static class SegmentWithTrueFalseTest {
    private ByteBuffersDirectory directory;

    /** Contains both true and false values in the segment. */
    private SortedDocValues dv;

    @Before
    public void setUp() throws Exception {
      this.directory = new ByteBuffersDirectory();
      IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig());
      writer.addDocuments(
          List.of(
              List.of(
                  new SortedDocValuesField(
                      FIELD, new BytesRef(FieldValue.BOOLEAN_TRUE_FIELD_VALUE))),
              List.of(
                  new SortedDocValuesField(
                      FIELD, new BytesRef(FieldValue.BOOLEAN_FALSE_FIELD_VALUE))),
              List.of(new NumericDocValuesField("OtherField", 5))));
      writer.commit();
      writer.close();
      DirectoryReader reader = DirectoryReader.open(this.directory);
      this.dv = DocValues.getSorted(Iterables.getOnlyElement(reader.leaves()).reader(), FIELD);
    }

    private MixedBooleanLeafComparator createComparator(Optional<BsonValue> top)
        throws IOException {
      return MixedBooleanLeafComparatorTest.createComparator(this.dv, top);
    }

    @After
    public void tearDown() throws Exception {
      this.directory.close();
    }

    @Test
    public void hasValue() throws IOException {
      var comparator = createComparator(Optional.empty());

      assertTrue(comparator.hasValue(0));
      assertEquals(BsonBoolean.TRUE, comparator.getCurrentValue());

      assertTrue(comparator.hasValue(1));
      assertEquals(BsonBoolean.FALSE, comparator.getCurrentValue());

      assertFalse(comparator.hasValue(2));
      // getCurrentValue() is now undefined.
    }

    @Test
    public void hasValueWithTop() throws IOException {
      var comparator = createComparator(Optional.of(BsonNull.VALUE));

      assertTrue(comparator.hasValue(0));
      assertEquals(BsonBoolean.TRUE, comparator.getCurrentValue());

      assertTrue(comparator.hasValue(1));
      assertEquals(BsonBoolean.FALSE, comparator.getCurrentValue());

      assertFalse(comparator.hasValue(2));
      // getCurrentValue() is now undefined.
    }

    @Test
    public void compareTopTrueExactMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonBoolean.TRUE));

      assertTrue(comparator.hasValue(0)); // 'T' i.e. true
      assertThat(comparator.compareTopToCurrent()).isEqualTo(0);

      assertTrue(comparator.hasValue(1)); // 'F' i.e. false
      assertThat(comparator.compareTopToCurrent()).isGreaterThan(0);
    }

    @Test
    public void compareTopFalseExactMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonBoolean.FALSE));

      assertTrue(comparator.hasValue(0)); // 'T' i.e. true
      assertThat(comparator.compareTopToCurrent()).isLessThan(0);

      assertTrue(comparator.hasValue(1)); // 'F' i.e. false
      assertThat(comparator.compareTopToCurrent()).isEqualTo(0);
    }

    @Test
    public void compareBottomTrueExactMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonUtils.MAX_KEY));
      comparator.notifyNewBottom(BsonBoolean.TRUE);

      assertTrue(comparator.hasValue(0)); // 'T' i.e. true
      assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);

      assertTrue(comparator.hasValue(1)); // 'F' i.e. false
      assertThat(comparator.compareBottomToCurrent()).isGreaterThan(0);
    }

    @Test
    public void compareBottomFalseExactMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonUtils.MAX_KEY));
      comparator.notifyNewBottom(BsonBoolean.FALSE);

      assertTrue(comparator.hasValue(0)); // 'T' i.e. true
      assertThat(comparator.compareBottomToCurrent()).isLessThan(0);

      assertTrue(comparator.hasValue(1)); // 'F' i.e. false
      assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);
    }
  }

  public static class SegmentWithOnlyTrueTest {
    private ByteBuffersDirectory directory;

    /** Contains only true values in the segment. */
    private SortedDocValues dv;

    @Before
    public void setUp() throws Exception {
      this.directory = new ByteBuffersDirectory();
      IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig());
      writer.addDocuments(
          List.of(
              List.of(
                  new SortedDocValuesField(
                      FIELD, new BytesRef(FieldValue.BOOLEAN_TRUE_FIELD_VALUE))),
              List.of(
                  new SortedDocValuesField(
                      FIELD, new BytesRef(FieldValue.BOOLEAN_TRUE_FIELD_VALUE))),
              List.of(new NumericDocValuesField("OtherField", 5))));
      writer.commit();
      writer.close();
      DirectoryReader reader = DirectoryReader.open(this.directory);
      this.dv = DocValues.getSorted(Iterables.getOnlyElement(reader.leaves()).reader(), FIELD);
    }

    private MixedBooleanLeafComparator createComparator(Optional<BsonValue> top)
        throws IOException {
      return MixedBooleanLeafComparatorTest.createComparator(this.dv, top);
    }

    @After
    public void tearDown() throws Exception {
      this.directory.close();
    }

    @Test
    public void hasValue() throws IOException {
      var comparator = createComparator(Optional.empty());

      assertTrue(comparator.hasValue(0));
      assertEquals(BsonBoolean.TRUE, comparator.getCurrentValue());

      assertTrue(comparator.hasValue(1));
      assertEquals(BsonBoolean.TRUE, comparator.getCurrentValue());
    }

    @Test
    public void hasValueWithTop() throws IOException {
      var comparator = createComparator(Optional.of(BsonNull.VALUE));

      assertTrue(comparator.hasValue(0));
      assertEquals(BsonBoolean.TRUE, comparator.getCurrentValue());

      assertTrue(comparator.hasValue(1));
      assertEquals(BsonBoolean.TRUE, comparator.getCurrentValue());
    }

    @Test
    public void compareTopExactMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonBoolean.TRUE));

      assertTrue(comparator.hasValue(0)); // 'T' i.e. true
      assertThat(comparator.compareTopToCurrent()).isEqualTo(0);

      assertTrue(comparator.hasValue(1)); // 'T' i.e. true
      assertThat(comparator.compareTopToCurrent()).isEqualTo(0);
    }

    @Test
    public void compareTopNoMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonBoolean.FALSE));

      assertTrue(comparator.hasValue(0)); // 'T' i.e. true
      assertThat(comparator.compareTopToCurrent()).isLessThan(0);

      assertTrue(comparator.hasValue(1)); // 'T' i.e. true
      assertThat(comparator.compareTopToCurrent()).isLessThan(0);
    }

    @Test
    public void compareBottomExactMatch() throws IOException {
      var comparator = createComparator(Optional.empty());
      comparator.notifyNewBottom(BsonBoolean.TRUE); // bottomOrd = 0

      assertTrue(comparator.hasValue(0)); // 'T' i.e. true
      assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);

      assertTrue(comparator.hasValue(1)); // 'T' i.e. true
      assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);
    }

    @Test
    public void compareBottomNoMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonUtils.MAX_KEY));
      comparator.notifyNewBottom(BsonBoolean.FALSE); // bottomOrd = -1 since false is not in segment

      assertTrue(comparator.hasValue(0)); // 'T' i.e. true
      assertThat(comparator.compareBottomToCurrent()).isLessThan(0);

      assertTrue(comparator.hasValue(1)); // 'T' i.e. true
      assertThat(comparator.compareBottomToCurrent()).isLessThan(0);
    }
  }

  public static class SegmentWithOnlyFalseTest {
    private ByteBuffersDirectory directory;

    /** Contains only false values in the segment. */
    private SortedDocValues dv;

    @Before
    public void setUp() throws Exception {
      this.directory = new ByteBuffersDirectory();
      IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig());
      writer.addDocuments(
          List.of(
              List.of(
                  new SortedDocValuesField(
                      FIELD, new BytesRef(FieldValue.BOOLEAN_FALSE_FIELD_VALUE))),
              List.of(
                  new SortedDocValuesField(
                      FIELD, new BytesRef(FieldValue.BOOLEAN_FALSE_FIELD_VALUE))),
              List.of(new NumericDocValuesField("OtherField", 5))));
      writer.commit();
      writer.close();
      DirectoryReader reader = DirectoryReader.open(this.directory);
      this.dv = DocValues.getSorted(Iterables.getOnlyElement(reader.leaves()).reader(), FIELD);
    }

    private MixedBooleanLeafComparator createComparator(Optional<BsonValue> top)
        throws IOException {
      return MixedBooleanLeafComparatorTest.createComparator(this.dv, top);
    }

    @After
    public void tearDown() throws Exception {
      this.directory.close();
    }

    @Test
    public void hasValue() throws IOException {
      var comparator = createComparator(Optional.empty());

      assertTrue(comparator.hasValue(0));
      assertEquals(BsonBoolean.FALSE, comparator.getCurrentValue());

      assertTrue(comparator.hasValue(1));
      assertEquals(BsonBoolean.FALSE, comparator.getCurrentValue());
    }

    @Test
    public void hasValueWithTop() throws IOException {
      var comparator = createComparator(Optional.of(BsonNull.VALUE));

      assertTrue(comparator.hasValue(0));
      assertEquals(BsonBoolean.FALSE, comparator.getCurrentValue());

      assertTrue(comparator.hasValue(1));
      assertEquals(BsonBoolean.FALSE, comparator.getCurrentValue());
    }

    @Test
    public void compareTopExactMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonBoolean.FALSE));

      assertTrue(comparator.hasValue(0)); // 'F' i.e. false
      assertThat(comparator.compareTopToCurrent()).isEqualTo(0);

      assertTrue(comparator.hasValue(1)); // 'F' i.e. false
      assertThat(comparator.compareTopToCurrent()).isEqualTo(0);
    }

    @Test
    public void compareTopNoMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonBoolean.TRUE));

      assertTrue(comparator.hasValue(0)); // 'F' i.e. false
      assertThat(comparator.compareTopToCurrent()).isGreaterThan(0);

      assertTrue(comparator.hasValue(1)); // 'F' i.e. false
      assertThat(comparator.compareTopToCurrent()).isGreaterThan(0);
    }

    @Test
    public void compareBottomExactMatch() throws IOException {
      var comparator = createComparator(Optional.of(BsonUtils.MAX_KEY));
      comparator.notifyNewBottom(BsonBoolean.FALSE); // bottomOrd = 0

      assertTrue(comparator.hasValue(0)); // 'F' i.e. false
      assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);

      assertTrue(comparator.hasValue(1)); // 'F' i.e. false
      assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);
    }

    @Test
    public void compareBottomNoMatch() throws IOException {
      var comparator = createComparator(Optional.empty());
      comparator.notifyNewBottom(
          BsonBoolean.TRUE); // bottomOrd = 1 since true is not in segment, but we pretend true = 1

      assertTrue(comparator.hasValue(0)); // 'F' i.e. false
      assertThat(comparator.compareBottomToCurrent()).isGreaterThan(0);

      assertTrue(comparator.hasValue(1)); // 'F' i.e. false
      assertThat(comparator.compareBottomToCurrent()).isGreaterThan(0);
    }
  }

  private static MixedBooleanLeafComparator createComparator(
      SortedDocValues dv, Optional<BsonValue> top) throws IOException {
    MixedFieldComparator fieldComparator =
        new MixedFieldComparator(
            FieldName.TypeField.BOOLEAN,
            BsonType.BOOLEAN,
            FieldPath.parse(FIELD),
            Optional.empty());

    int unused = 5;
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {fieldComparator}, UserFieldSortOptions.DEFAULT_ASC, unused);

    top.ifPresent(compositeComparator::setTopValue);
    return new MixedBooleanLeafComparator(compositeComparator, dv);
  }
}
