package com.xgen.mongot.index.version;

import org.junit.Assert;
import org.junit.Test;

public class FormatVersionTest {

  @Test
  public void of_readsTheNumberInTheEnginesLine() {
    Assert.assertSame(IndexFormatVersion.SIX, FormatVersion.of(IndexEngine.LUCENE, 6));
    Assert.assertEquals(
        new VectorliteFormatVersion(6), FormatVersion.of(IndexEngine.VECTORLITE, 6));
    Assert.assertNotEquals(
        FormatVersion.of(IndexEngine.LUCENE, 6), FormatVersion.of(IndexEngine.VECTORLITE, 6));
  }

  @Test
  public void current_isEachEnginesCurrent() {
    Assert.assertSame(IndexFormatVersion.CURRENT, FormatVersion.current(IndexEngine.LUCENE));
    Assert.assertSame(
        VectorliteFormatVersion.CURRENT, FormatVersion.current(IndexEngine.VECTORLITE));
    Assert.assertEquals(IndexEngine.LUCENE, IndexFormatVersion.CURRENT.engine());
    Assert.assertEquals(IndexEngine.VECTORLITE, VectorliteFormatVersion.CURRENT.engine());
  }

  @Test
  public void typedViews_refuseTheOtherEngine() {
    FormatVersion lucene = IndexFormatVersion.CURRENT;
    FormatVersion vectorlite = VectorliteFormatVersion.CURRENT;
    Assert.assertSame(IndexFormatVersion.CURRENT, lucene.asLucene());
    Assert.assertSame(VectorliteFormatVersion.CURRENT, vectorlite.asVectorlite());
    Assert.assertThrows(UnsupportedOperationException.class, lucene::asVectorlite);
    Assert.assertThrows(UnsupportedOperationException.class, vectorlite::asLucene);
  }
}
