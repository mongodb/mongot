package com.xgen.mongot.index.query.highlights;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.query.highlights.HighlightBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class HighlightTest {

  @Test
  public void resolvedLuceneFieldNamesAndStoredLuceneFieldNameMap() {
    Highlight highlight =
        HighlightBuilder.builder().path("a").storedPath("b").path("c").storedPath("d").build();

    Assert.assertEquals(
        List.of(
            FieldName.TypeField.STRING.getLuceneFieldName(FieldPath.newRoot("a"), Optional.empty()),
            FieldName.TypeField.STRING.getLuceneFieldName(
                FieldPath.newRoot("c"), Optional.empty())),
        highlight.resolvedLuceneFieldNames());

    Assert.assertEquals(
        Map.of(
            FieldName.TypeField.STRING.getLuceneFieldName(FieldPath.newRoot("a"), Optional.empty()),
            FieldName.TypeField.STRING.getLuceneFieldName(FieldPath.newRoot("b"), Optional.empty()),
            FieldName.TypeField.STRING.getLuceneFieldName(FieldPath.newRoot("c"), Optional.empty()),
            FieldName.TypeField.STRING.getLuceneFieldName(
                FieldPath.newRoot("d"), Optional.empty())),
        highlight.storedLuceneFieldNameMap());
  }
}
