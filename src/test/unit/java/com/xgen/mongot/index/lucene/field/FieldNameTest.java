package com.xgen.mongot.index.lucene.field;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.field.FieldName.MultiField;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

public class FieldNameTest {

  @Test
  public void testMultiFieldIsTypeOf() {
    assertTrue(MultiField.isTypeOf("$multi/fieldWithMultiAnalyzer.a"));
    assertTrue(MultiField.isTypeOf("$embedded:4/root/$multi/field.multi"));
    assertFalse(MultiField.isTypeOf("$embedded:8/teachers/$type:string/teachers.classes.subject"));
  }

  @Test
  public void testGetMultiFromLuceneField() {
    StringMultiFieldPath result = MultiField.getFieldPath("$multi/fieldWithMultiAnalyzer.a");
    assertEquals("fieldWithMultiAnalyzer", result.getFieldPath().toString());
    assertEquals("a", result.getMulti());
  }

  @Test
  public void testGetMultiFromEmbeddedLuceneField() {
    String luceneField = "$embedded:4/root/$multi/field.multi";
    StringMultiFieldPath result = MultiField.getFieldPath(luceneField);
    assertEquals("field", result.getFieldPath().toString());
    assertEquals("multi", result.getMulti());
  }

  @Test
  public void testGetMultiFromInvalidLuceneField() {
    assertThrows(
        IllegalArgumentException.class,
        () -> MultiField.getFieldPath("$multi/fieldWithMultiAnalyzer."));
    assertThrows(
        AssertionError.class,
        () ->
            MultiField.getFieldPath("$embedded:8/teachers/$type:string/teachers.classes.subject"));
  }

  @Test
  public void testIsTypeForTypeWithSamePrefix() {
    String doubleMultiplePath =
        FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(
            FieldPath.parse("path"), Optional.empty());
    assertEquals("$type:doubleMultiple/path", doubleMultiplePath);
    assertEquals(
        "$type:double/path",
        FieldName.TypeField.NUMBER_DOUBLE.getLuceneFieldName(
            FieldPath.parse("path"), Optional.empty()));

    assertFalse(FieldName.TypeField.NUMBER_DOUBLE.isTypeOf(doubleMultiplePath));
  }

  @Test
  public void testGetLuceneFieldNameForPathByStrings() {
    assertEquals(
        "$type:string/path",
        FieldName.getLuceneFieldNameForStringPath(
            StringPathBuilder.fieldPath("path"), Optional.empty()));
    assertEquals(
        "$type:string/a.b.c",
        FieldName.getLuceneFieldNameForStringPath(
            StringPathBuilder.fieldPath("a.b.c"), Optional.empty()));
    assertEquals(
        "$multi/a.b.c.multi",
        FieldName.getLuceneFieldNameForStringPath(
            StringPathBuilder.withMulti("a.b.c", "multi"), Optional.empty()));
  }

  @Test
  public void testGetLuceneFieldNameForPathByStringPath() {
    @Var StringPath stringPath = StringPathBuilder.fieldPath("a.b.c");
    assertEquals(
        "$type:string/a.b.c",
        FieldName.getLuceneFieldNameForStringPath(stringPath, Optional.empty()));

    stringPath = StringPathBuilder.withMulti("a.b.c", "multi");
    assertEquals(
        "$multi/a.b.c.multi",
        FieldName.getLuceneFieldNameForStringPath(stringPath, Optional.empty()));
  }

  @Test
  public void testFromLuceneBooleanFieldName() {
    StringPath stringPath = StringPathBuilder.fieldPath("a.b.c");

    assertEquals(
        "$type:boolean/a.b.c",
        FieldName.TypeField.BOOLEAN.getLuceneFieldName(
            stringPath.asField().getValue(), Optional.empty()));
  }

  @Test
  public void testFromLuceneObjectIdFieldName() {
    StringPath stringPath = StringPathBuilder.fieldPath("a.b.c");

    assertEquals(
        "$type:objectId/a.b.c",
        FieldName.TypeField.OBJECT_ID.getLuceneFieldName(
            stringPath.asField().getValue(), Optional.empty()));
  }

  @Test
  public void testPrependEmbeddedPathAbsent() {
    String fieldName = "$type:string/teachers.classes.subject";
    Optional<FieldPath> embeddedRoot = Optional.empty();

    String expected = "$type:string/teachers.classes.subject";
    assertEquals(expected, FieldName.EmbeddedField.prependPrefix(fieldName, embeddedRoot));
  }

  @Test
  public void testPrependEmbeddedPathPresent() {
    String fieldName = "$type:string/teachers.classes.subject";
    Optional<FieldPath> embeddedRoot = Optional.of(FieldPath.parse("teachers.classes"));

    String expected = "$embedded:16/teachers.classes/$type:string/teachers.classes.subject";
    assertEquals(expected, FieldName.EmbeddedField.prependPrefix(fieldName, embeddedRoot));
  }

  @Test
  public void testPrependEmbeddedPathMultiCodePointRoot() {
    String fieldName = "$type:string/résume.B.☃.💩.subject";

    // résume == 7 code units
    // B == 1 code unit
    // ☃ == 3 code units
    // 💩 == 4 code units
    // 7 + 1 + 3 + 4 (+ 3 to count 3 '.' characters) == 18
    Optional<FieldPath> embeddedRoot = Optional.of(FieldPath.parse("résume.B.☃.💩"));

    String expected = "$embedded:18/résume.B.☃.💩/$type:string/résume.B.☃.💩.subject";
    assertEquals(expected, FieldName.EmbeddedField.prependPrefix(fieldName, embeddedRoot));
  }

  @Test
  public void testStripEmbeddedPrefixWhenPrefixAbsent() {
    String fieldName = "$type:string/teachers.classes.subject";

    String expected = "$type:string/teachers.classes.subject";
    assertEquals(expected, FieldName.EmbeddedField.stripPrefix(fieldName));
  }

  @Test
  public void testStripEmbeddedPrefixWhenPrefixPresent() {
    String fieldName = "$embedded:16/teachers.classes/$type:string/teachers.classes.subject";

    String expected = "$type:string/teachers.classes.subject";
    assertEquals(expected, FieldName.EmbeddedField.stripPrefix(fieldName));
  }

  @Test
  public void testStripEmbeddedPrefixWhenPrefixHasVaryingCodePointSizes() {
    String fieldName = "$embedded:18/résume.B.☃.💩/$type:string/résume.B.☃.💩.subject";

    String expected = "$type:string/résume.B.☃.💩.subject";
    assertEquals(expected, FieldName.EmbeddedField.stripPrefix(fieldName));
  }

  @Test
  public void testEmbeddedPrefixRoundTrips() {
    List<String> unprefixedFieldNames =
        List.of("$type:string/teachers.classes", "$type:number/asdf", ".", " ", "$$");
    List<FieldPath> embeddedRoots =
        List.of(".", " ", "\t", "    ", "a", "01234567890", ".......", "カｶ", "/", "$", "").stream()
            .map(FieldPath::parse)
            .collect(Collectors.toList());

    for (String unprefixedFieldName : unprefixedFieldNames) {
      for (FieldPath embeddedRoot : embeddedRoots) {
        assertEquals(
            FieldName.EmbeddedField.stripPrefix(
                FieldName.EmbeddedField.prependPrefix(
                    unprefixedFieldName, Optional.of(embeddedRoot))),
            unprefixedFieldName);
      }
    }
  }

  @Test
  public void testEmbeddedPrefixRoundTripsRandomStressTest() {
    for (int i = 0; i != 1000; i++) {
      String unPrefixedFieldName = RandomStringUtils.random(RandomUtils.nextInt(0, 2000));
      FieldPath embeddedRoot =
          FieldPath.parse(RandomStringUtils.random(RandomUtils.nextInt(0, 2000)));

      assertEquals(
          FieldName.EmbeddedField.stripPrefix(
              FieldName.EmbeddedField.prependPrefix(
                  unPrefixedFieldName, Optional.of(embeddedRoot))),
          unPrefixedFieldName);
    }
  }

  @Test
  public void testStripAnyPrefixFromLuceneFieldName() {
    assertEquals(
        "résume.B.☃.💩.subject",
        FieldName.stripAnyPrefixFromLuceneFieldName(
            "$embedded:18/résume.B.☃.💩/$type:string/résume.B.☃.💩.subject"));
    assertEquals(
        "résume.B.☃.💩.subject",
        FieldName.stripAnyPrefixFromLuceneFieldName("$type:string/résume.B.☃.💩.subject"));

    assertEquals(
        "teachers.classes.subject",
        FieldName.stripAnyPrefixFromLuceneFieldName(
            "$embedded:16/teachers.classes/$type:string/teachers.classes.subject"));
    assertEquals(
        "teachers.classes.subject",
        FieldName.stripAnyPrefixFromLuceneFieldName("$type:string/teachers.classes.subject"));

    assertEquals(
        "teachers.firstName",
        FieldName.stripAnyPrefixFromLuceneFieldName(
            "$embedded:8/teachers/$type:string/teachers.firstName"));
    assertEquals(
        "teachers.firstName",
        FieldName.stripAnyPrefixFromLuceneFieldName("$type:string/teachers.firstName"));
  }

  @Test
  public void testIsAtEmbeddedRoot() {
    Assert.assertTrue(
        FieldName.EmbeddedField.isAtEmbeddedRoot(Optional.of(FieldPath.parse("teachers")))
            .test("$embedded:8/teachers/$type:string/teachers.firstName"));
    assertFalse(
        FieldName.EmbeddedField.isAtEmbeddedRoot(Optional.of(FieldPath.parse("teachers.firstName")))
            .test("$embedded:8/teachers/$type:string/teachers.firstName"));
    assertFalse(
        FieldName.EmbeddedField.isAtEmbeddedRoot(Optional.empty())
            .test("$embedded:8/teachers/$type:string/teachers.firstName"));

    Assert.assertTrue(
        FieldName.EmbeddedField.isAtEmbeddedRoot(Optional.of(FieldPath.parse("résume.B.☃.💩")))
            .test("$embedded:18/résume.B.☃.💩/$type:string/résume.B.☃.💩.subject"));
    assertFalse(
        FieldName.EmbeddedField.isAtEmbeddedRoot(Optional.of(FieldPath.parse("résume.B.☃.💩1")))
            .test("$embedded:18/résume.B.☃.💩/$type:string/résume.B.☃.💩.subject"));
    assertFalse(
        FieldName.EmbeddedField.isAtEmbeddedRoot(Optional.empty())
            .test("$embedded:18/résume.B.☃.💩/$type:string/résume.B.☃.💩.subject"));

    assertFalse(
        FieldName.EmbeddedField.isAtEmbeddedRoot(Optional.of(FieldPath.parse("teachers")))
            .test("$type:string/teachers.firstName"));
    assertFalse(
        FieldName.EmbeddedField.isAtEmbeddedRoot(Optional.of(FieldPath.parse("teachers.firstName")))
            .test("$type:string/teachers.firstName"));
    Assert.assertTrue(
        FieldName.EmbeddedField.isAtEmbeddedRoot(Optional.empty())
            .test("$type:string/teachers.firstName"));
  }

  @Test
  public void testGetPrefixFromLuceneFieldNameForTypeFieldOrMultiField() {
    assertEquals(
        "$embedded:6/field0/$multi/",
        FieldName.getPrefixFromLuceneFieldNameForTypeFieldOrMultiField(
            "$embedded:6/field0/$multi/field0.multi0"));
    assertEquals(
        "$embedded:6/field0/$type:string/",
        FieldName.getPrefixFromLuceneFieldNameForTypeFieldOrMultiField(
            "$embedded:6/field0/$type:string/field0.a"));
    assertEquals(
        "$type:string/",
        FieldName.getPrefixFromLuceneFieldNameForTypeFieldOrMultiField(
            "$type:string/field0.a.b.c"));
    assertEquals(
        "$multi/",
        FieldName.getPrefixFromLuceneFieldNameForTypeFieldOrMultiField(
            "$multi/field0.field1.multi0"));
  }

  @Test
  public void testExtractComponents_NonEmbeddedTypeField_Simple() {
    String luceneFieldName = "$type:string/simpleField";
    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isEmpty());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isEmpty());
    assertEquals(FieldPath.parse("simpleField"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_NonEmbeddedTypeField_Nested() {
    String luceneFieldName = "$type:int64/nested.path.field";
    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isEmpty());
    assertEquals(FieldName.TypeField.NUMBER_INT64, components.typeField());
    assertTrue(components.multiFieldPath().isEmpty());
    assertEquals(FieldPath.parse("nested.path.field"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_NonEmbeddedTypeField_SingleCharFieldPath() {
    String luceneFieldName = "$type:string/a";
    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isEmpty());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isEmpty());
    assertEquals(FieldPath.parse("a"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_NonEmbeddedMultiField_Simple() {
    String luceneFieldName = "$multi/field.analysis";
    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isEmpty());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isPresent());
    assertEquals("field", components.multiFieldPath().get().getFieldPath().toString());
    assertEquals("analysis", components.multiFieldPath().get().getMulti());
    assertEquals(FieldPath.parse("field"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_NonEmbeddedMultiField_Nested() {
    String luceneFieldName = "$multi/path.to.data.custom_multi";
    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isEmpty());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isPresent());
    assertEquals("path.to.data", components.multiFieldPath().get().getFieldPath().toString());
    assertEquals("custom_multi", components.multiFieldPath().get().getMulti());
    assertEquals(FieldPath.parse("path.to.data"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_NonEmbeddedMultiField_SingleCharFieldPath() {
    String luceneFieldName = "$multi/a.b";
    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isEmpty());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isPresent());
    assertEquals("a", components.multiFieldPath().get().getFieldPath().toString());
    assertEquals("b", components.multiFieldPath().get().getMulti());
    assertEquals(FieldPath.parse("a"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_NonEmbeddedMultiField_MultiByteMultiName() {
    String luceneFieldName = "$multi/field.☃";
    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isEmpty());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isPresent());
    assertEquals("field", components.multiFieldPath().get().getFieldPath().toString());
    assertEquals("☃", components.multiFieldPath().get().getMulti());
    assertEquals(FieldPath.parse("field"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_EmbeddedTypeField_SimpleRootAndPath() {
    String luceneFieldName = "$embedded:3/foo/$type:string/bar";
    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isPresent());
    assertEquals(FieldPath.parse("foo"), components.embeddedRoot().get());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isEmpty());
    assertEquals(FieldPath.parse("bar"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_EmbeddedTypeField_NestedRootAndPath() {
    // "a.b.c" is 5 chars, but 5 UTF-8 bytes if all ASCII.
    // Example from FieldName.java indicates UTF-8 length for embedded root prefix.
    String embeddedRootStr = "my.embedded.doc"; // 15 chars, 15 bytes in UTF-8
    int embeddedRootUtf8Length =
        embeddedRootStr.getBytes(StandardCharsets.UTF_8).length; // Should be 15
    String luceneFieldName =
        "$embedded:"
            + embeddedRootUtf8Length
            + "/"
            + embeddedRootStr
            + "/$type:date/"
            + embeddedRootStr
            + ".dateField";

    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isPresent());
    assertEquals(FieldPath.parse(embeddedRootStr), components.embeddedRoot().get());
    assertEquals(FieldName.TypeField.DATE, components.typeField());
    assertTrue(components.multiFieldPath().isEmpty());
    assertEquals(FieldPath.parse(embeddedRootStr + ".dateField"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_EmbeddedTypeField_MultiByteRoot() {
    // "résume.B.☃.💩" has UTF-8 length 18 (as per existing test)
    String embeddedRootStr = "résume.B.☃.💩";
    int embeddedRootUtf8Length =
        embeddedRootStr.getBytes(StandardCharsets.UTF_8).length; // Should be 18
    String luceneFieldName =
        "$embedded:" + embeddedRootUtf8Length + "/" + embeddedRootStr + "/$type:string/some.field";

    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isPresent());
    assertEquals(FieldPath.parse(embeddedRootStr), components.embeddedRoot().get());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isEmpty());
    assertEquals(FieldPath.parse("some.field"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_EmbeddedMultiField_SimpleRootAndPath() {
    String luceneFieldName = "$embedded:4/root/$multi/nested.multi0";
    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isPresent());
    assertEquals(FieldPath.parse("root"), components.embeddedRoot().get());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isPresent());
    assertEquals("nested", components.multiFieldPath().get().getFieldPath().toString());
    assertEquals("multi0", components.multiFieldPath().get().getMulti());
    assertEquals(FieldPath.parse("nested"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_EmbeddedMultiField_NestedRootAndPath() {
    String embeddedRootStr = "parent.emb";
    int embeddedRootUtf8Length = embeddedRootStr.getBytes(StandardCharsets.UTF_8).length;
    String luceneFieldName =
        "$embedded:"
            + embeddedRootUtf8Length
            + "/"
            + embeddedRootStr
            + "/$multi/sub.field.multiXYZ";

    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isPresent());
    assertEquals(FieldPath.parse(embeddedRootStr), components.embeddedRoot().get());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isPresent());
    assertEquals("sub.field", components.multiFieldPath().get().getFieldPath().toString());
    assertEquals("multiXYZ", components.multiFieldPath().get().getMulti());
    assertEquals(FieldPath.parse("sub.field"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_EmbeddedMultiField_MultiByteRoot() {
    String embeddedRootStr = "数据.embedded"; // Example: "数据" is 2 chars, 6 bytes in UTF-8
    int embeddedRootUtf8Length =
        embeddedRootStr.getBytes(StandardCharsets.UTF_8).length; // Should be 15 for "数据.embedded"
    String luceneFieldName =
        "$embedded:" + embeddedRootUtf8Length + "/" + embeddedRootStr + "/$multi/my.data.analyser";

    FieldName.Components components = FieldName.extractFieldNameComponents(luceneFieldName);

    assertTrue(components.embeddedRoot().isPresent());
    assertEquals(FieldPath.parse(embeddedRootStr), components.embeddedRoot().get());
    assertEquals(FieldName.TypeField.STRING, components.typeField());
    assertTrue(components.multiFieldPath().isPresent());
    assertEquals("my.data", components.multiFieldPath().get().getFieldPath().toString());
    assertEquals("analyser", components.multiFieldPath().get().getMulti());
    assertEquals(FieldPath.parse("my.data"), components.fieldPath());
  }

  @Test
  public void testExtractComponents_InvalidFormat_NoPrefix() {
    String luceneFieldName = "just.a.field.name";
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "The luceneFieldName is not a recognized "
                    + "top-level TypeField or MultiField format"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_MalformedTypePrefix() {
    String luceneFieldName = "$typo:string/field"; // Typo in $type
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "The luceneFieldName is not a recognized "
                    + "top-level TypeField or MultiField format"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_MalformedMultiPrefix() {
    String luceneFieldName = "$mul:field.name"; // Typo in $multi
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "The luceneFieldName is not a recognized "
                    + "top-level TypeField or MultiField format"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_MetaField() {
    String luceneFieldName = "$meta/id"; // Not a TypeField or MultiField
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "The luceneFieldName is not a recognized "
                    + "top-level TypeField or MultiField format"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_StaticField() {
    String luceneFieldName = "$storedSource"; // Not a TypeField or MultiField
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "The luceneFieldName is not a recognized "
                    + "top-level TypeField or MultiField format"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_NonEmbeddedMultiFieldSuffixMissingName() {
    String luceneFieldName = "$multi/field."; // Missing multi name
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown.getMessage().contains("multi path did not have a multi name after the final ."));
  }

  @Test
  public void testExtractComponents_InvalidFormat_NonEmbeddedMultiFieldNoDot() {
    String luceneFieldName = "$multi/fieldonly"; // Multi field needs a dot for multi name
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(thrown.getMessage().contains("multi path did not contain a ."));
  }

  @Test
  public void testExtractComponents_InvalidFormat_EmbeddedMissingTypeOrMultiSuffix() {
    String luceneFieldName = "$embedded:3/foo/bar"; // Missing $type or $multi after embedded root
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Embedded field name does not contain a valid TypeField or MultiField suffix"));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Field name does not contain a valid TypeField or MultiField component: bar"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_EmbeddedMalformedTypeSuffix() {
    String luceneFieldName = "$embedded:3/foo/$typo:string/bar"; // Typo in type after embedded
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Embedded field name does not contain a valid TypeField or MultiField suffix"));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Field name does not contain a valid "
                    + "TypeField or MultiField component: $typo:string/bar"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_EmbeddedMalformedMultiSuffix() {
    String luceneFieldName = "$embedded:3/foo/$mul:field.name"; // Typo in multi after embedded
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Embedded field name does not contain a valid TypeField or MultiField suffix"));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Field name does not contain a valid "
                    + "TypeField or MultiField component: $mul:field.name"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_EmbeddedMetaFieldSuffix() {
    String luceneFieldName = "$embedded:3/foo/$meta/id"; // Invalid field type after embedded
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Embedded field name does not contain a valid TypeField or MultiField suffix"));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Field name does not contain a valid TypeField or MultiField component: $meta/id"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_EmbeddedStaticFieldSuffix() {
    String luceneFieldName = "$embedded:3/foo/$storedSource"; // Invalid field type after embedded
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Embedded field name does not contain a valid TypeField or MultiField suffix"));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Field name does not contain a valid "
                    + "TypeField or MultiField component: $storedSource"));
  }

  @Test
  public void testExtractComponents_InvalidFormat_MalformedEmbeddedLength() {
    String luceneFieldName = "$embedded:abc/foo/$type:string/bar"; // Invalid length "abc"
    assertThrows(
        IllegalArgumentException.class,
        () -> FieldName.extractFieldNameComponents(luceneFieldName));
  }

  @Test
  public void testExtractComponents_InvalidFormat_MismatchedEmbeddedLength() {
    String luceneFieldName = "$embedded:2/foo/$type:string/bar"; // Root "foo" has length 3, not 2
    assertThrows(
        IllegalArgumentException.class,
        () -> FieldName.extractFieldNameComponents(luceneFieldName));
  }

  @Test
  public void testExtractComponents_InvalidFormat_MissingRootDelimiter() {
    String luceneFieldName = "$embedded:3foo/$type:string/bar"; // Missing '/' after length
    assertThrows(
        IllegalArgumentException.class,
        () -> FieldName.extractFieldNameComponents(luceneFieldName));
  }

  @Test
  public void testExtractComponents_InvalidFormat_MissingEmbeddedRootSegment() {
    String luceneFieldName =
        "$embedded:3/$type:string/bar"; // Root specified as 3 bytes, but segment is empty
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldName.extractFieldNameComponents(luceneFieldName));
    assertTrue(
        thrown.getMessage().contains("String index out of range: -1")
            || thrown
                .getMessage()
                .contains("Field name does not contain a valid TypeField or MultiField component"));
  }

  @Test
  public void testGetEmbeddedRootPathOrThrow() {
    assertThat(FieldName.getEmbeddedRootPathOrThrow("$embedded:6/field0/$multi/field0.multi0"))
        .isEqualTo("field0");
    assertThat(FieldName.getEmbeddedRootPathOrThrow("$embedded:0//$multi/field0.multi0"))
        .isEqualTo("");
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> FieldName.getEmbeddedRootPathOrThrow("invalidFieldPath/asdf/field1"));
  }

  @Test
  public void getTypeOf_validPrefixes_returnsType() {
    for (FieldName.TypeField typeField : FieldName.TypeField.values()) {
      String fieldName =
          typeField.getLuceneFieldName(FieldPath.parse("someField"), Optional.empty());
      Optional<FieldName.TypeField> result = FieldName.TypeField.getTypeOf(fieldName);
      assertThat(result).hasValue(typeField);
    }
  }

  @Test
  public void getTypeOf_validPrefixesWithEmbeddedRoot_returnsType() {
    for (FieldName.TypeField typeField : FieldName.TypeField.values()) {
      String luceneFieldName =
          typeField.getLuceneFieldName(
              FieldPath.parse("someField"), Optional.of(FieldPath.newRoot("/:$")));

      Optional<FieldName.TypeField> result = FieldName.TypeField.getTypeOf(luceneFieldName);

      assertThat(result).hasValue(typeField);
    }
  }

  @Test
  public void getTypeOf_validPrefixesWithDeeplyNestedEmbeddedRoot_returnsType() {
    for (FieldName.TypeField typeField : FieldName.TypeField.values()) {
      String embeddedRoot = "/nested/root/level/1/2/3";
      String luceneFieldName =
          typeField.getLuceneFieldName(
              FieldPath.parse("nestedField"), Optional.of(FieldPath.newRoot(embeddedRoot)));

      Optional<FieldName.TypeField> result = FieldName.TypeField.getTypeOf(luceneFieldName);
      assertThat(result).hasValue(typeField);
    }
  }

  @Test
  public void getTypeOf_validPrefixesWithSpecialCharacterEmbeddedRoot_returnsType() {
    for (FieldName.TypeField typeField : FieldName.TypeField.values()) {
      // Embedded root with special characters
      String embeddedRoot = "/meta+data~name";
      String luceneFieldName =
          typeField.getLuceneFieldName(
              FieldPath.parse("specialField"), Optional.of(FieldPath.newRoot(embeddedRoot)));

      Optional<FieldName.TypeField> result = FieldName.TypeField.getTypeOf(luceneFieldName);

      assertThat(result).hasValue(typeField);
    }
  }

  @Test
  public void getTypeOf_invalidEmbeddedPrefixes_returnsEmpty() {
    for (FieldName.TypeField typeField : FieldName.TypeField.values()) {
      // Invalid embedded root
      String luceneFieldName =
          "$embedded:0/invalidRoot/$type:" + typeField.name().toLowerCase() + "/someField";
      Optional<FieldName.TypeField> result = FieldName.TypeField.getTypeOf(luceneFieldName);

      // Result should be empty for invalid embedded paths
      assertThat(result).isEmpty();
    }
  }

  @Test
  public void getTypeOf_invalidPrefixes_returnsEmpty() {
    assertThat(FieldName.TypeField.getTypeOf("$invalid:prefix/field")).isEmpty();
    assertThat(FieldName.TypeField.getTypeOf("random:prefix/field")).isEmpty();
    assertThat(FieldName.TypeField.getTypeOf("$type:/missingField")).isEmpty();
    assertThat(FieldName.TypeField.getTypeOf("$storedSource")).isEmpty();
    // Valid MetaField, but should return empty since non-TypeField
    assertThat(FieldName.TypeField.getTypeOf("$meta/_id")).isEmpty();
    assertThat(FieldName.TypeField.getTypeOf("$meta/embeddedRoot")).isEmpty();
    // Invalid or unknown prefixes
    assertThat(FieldName.TypeField.getTypeOf("$multi/someMultiFieldData")).isEmpty();
  }

  @Test
  public void getTypeOf_substringEdgeCasesInvalidPrefixes_returnsEmpty() {
    // Case: No delimiter
    assertThat(FieldName.TypeField.getTypeOf("$type:uuid")).isEmpty();

    // Case: Delimiter exists, but there's no content after the delimiter
    assertThat(FieldName.TypeField.getTypeOf("$type:string/")).isEmpty();

    // Case: Non-$type prefix
    assertThat(FieldName.TypeField.getTypeOf("$invalidPrefix/someField")).isEmpty();

    // Case: Malformed combination
    assertThat(FieldName.TypeField.getTypeOf("$type:/")).isEmpty();
  }

  @Test
  public void getTypeOf_validPrefixWithContent_returnsType() {
    Optional<FieldName.TypeField> result = FieldName.TypeField.getTypeOf("$type:string/someField");
    assertThat(result).hasValue(FieldName.TypeField.STRING);
  }

  @Test
  public void getTypeOf_emptyFieldName_returnsEmpty() {
    Optional<FieldName.TypeField> result = FieldName.TypeField.getTypeOf("");
    assertThat(result).isEmpty();
  }

  @Test
  public void isNullnessFieldName_variousInputs_correctResults() {
    assertTrue(FieldName.isNullnessFieldName("$meta/nullness/age"));
    assertTrue(FieldName.isNullnessFieldName("$meta/nullness/nested.field.path"));
    assertFalse(FieldName.isNullnessFieldName("$meta/nullness"));
    assertFalse(FieldName.isNullnessFieldName("$meta/_id"));
    assertFalse(FieldName.isNullnessFieldName("$type:int64/v2/age"));
    assertFalse(FieldName.isNullnessFieldName(null));
  }
}
