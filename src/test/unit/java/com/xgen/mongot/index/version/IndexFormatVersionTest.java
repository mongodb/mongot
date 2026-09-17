package com.xgen.mongot.index.version;

import com.xgen.testing.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class IndexFormatVersionTest {

  @Test
  public void testBackendIndexVersionEquals() {
    TestUtils.assertEqualityGroups(
        () -> IndexFormatVersion.create(1), () -> IndexFormatVersion.create(2));
  }

  @Test
  public void versionMustBePositive() {
    Assert.assertThrows(IllegalArgumentException.class, () -> IndexFormatVersion.create(0));
    Assert.assertThrows(IllegalArgumentException.class, () -> IndexFormatVersion.create(-1));
    IndexFormatVersion.create(1);
  }

  @Test
  public void testConstantVersionFields() {
    Assert.assertEquals(
        1,
        IndexFormatVersion.CURRENT.versionNumber
            - IndexFormatVersion.MIN_SUPPORTED_VERSION.versionNumber);
  }

  @Test
  public void testEquality() {
    Assert.assertEquals(IndexFormatVersion.FIVE, IndexFormatVersion.create(5));
    Assert.assertEquals(IndexFormatVersion.SIX, IndexFormatVersion.create(6));
  }

  /** A version this build has no constant for parses, keeps its number, and is unsupported. */
  @Test
  public void create_unknownVersion_keepsItsNumberAndIsUnsupported() {
    int number = IndexFormatVersion.MAX_SUPPORTED_VERSION.versionNumber + 1;
    IndexFormatVersion version = IndexFormatVersion.create(number);
    Assert.assertEquals(number, version.versionNumber);
    Assert.assertFalse(version.isCurrent());
    Assert.assertFalse(version.isSupported());
    Assert.assertEquals(0, version.minVectorFeatureVersion());
    Assert.assertEquals(0, version.minSearchFeatureVersion());
  }

  @Test
  public void minVectorFeatureVersion_currentAndPreviousFormatVersions_equalsTo3() {
    Assert.assertEquals(3, IndexFormatVersion.FIVE.minVectorFeatureVersion());
    Assert.assertEquals(3, IndexFormatVersion.SIX.minVectorFeatureVersion());
    Assert.assertEquals(3, IndexFormatVersion.CURRENT.minVectorFeatureVersion());
  }
}
