package com.xgen.mongot.index.ingestion.handlers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertSame;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

public class LuceneDocumentUtilTest {

  @Test
  public void add() {
    Document left = new Document();
    Document right = new Document();
    var leftField = new SortedSetDocValuesField("left", new BytesRef());
    var rightField = new SortedSetDocValuesField("right", new BytesRef());
    left.add(leftField);
    right.add(rightField);

    Document result = LuceneDocumentUtil.add(left, right);

    assertSame(left, result);
    assertThat(left).containsExactly(leftField, rightField);
    assertThat(right).containsExactly(rightField);
  }
}
