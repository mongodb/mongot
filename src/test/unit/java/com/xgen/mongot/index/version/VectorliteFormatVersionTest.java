package com.xgen.mongot.index.version;

import org.junit.Assert;
import org.junit.Test;

public class VectorliteFormatVersionTest {

  /** A literal, so a bump is a deliberate edit here too: it rebuilds every vectorlite index. */
  @Test
  public void current_isPinned() {
    Assert.assertEquals(1, VectorliteFormatVersion.CURRENT.versionNumber());
  }

  @Test
  public void minSupported_equalsCurrent() {
    Assert.assertSame(
        VectorliteFormatVersion.CURRENT, VectorliteFormatVersion.MIN_SUPPORTED_VERSION);
    Assert.assertTrue(VectorliteFormatVersion.CURRENT.isCurrent());
    Assert.assertTrue(VectorliteFormatVersion.CURRENT.isSupported());

    var next = new VectorliteFormatVersion(VectorliteFormatVersion.CURRENT.versionNumber() + 1);
    Assert.assertFalse(next.isCurrent());
    Assert.assertFalse(next.isSupported());
  }

  @Test
  public void versionMustBePositive() {
    Assert.assertThrows(IllegalArgumentException.class, () -> new VectorliteFormatVersion(0));
    Assert.assertThrows(IllegalArgumentException.class, () -> new VectorliteFormatVersion(-1));
  }
}
