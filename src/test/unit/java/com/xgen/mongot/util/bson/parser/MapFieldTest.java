package com.xgen.mongot.util.bson.parser;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class MapFieldTest {

  @Test
  public void parsesSingleValueList() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Map.ofEntries(Map.entry("foo", 1)),
            Map.ofEntries(Map.entry("bar", 2)),
            (name, doc, value) ->
                doc.append(
                    name,
                    new BsonDocument(
                        value.entrySet().stream()
                            .map(e -> new BsonElement(e.getKey(), new BsonInt32(e.getValue())))
                            .collect(Collectors.toList()))),
            name -> Field.builder(name).mapOf(Value.builder().intValue().required()));
    test.test();
  }

  @Test
  public void parsesMultiValueList() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Map.ofEntries(Map.entry("foo", 1), Map.entry("bar", 2)),
            Map.ofEntries(Map.entry("baz", 3), Map.entry("buzz", 4)),
            (name, doc, value) ->
                doc.append(
                    name,
                    new BsonDocument(
                        value.entrySet().stream()
                            .map(e -> new BsonElement(e.getKey(), new BsonInt32(e.getValue())))
                            .collect(Collectors.toList()))),
            name -> Field.builder(name).mapOf(Value.builder().intValue().required()));
    test.test();
  }

  @Test
  public void parsersEmptyMap() throws Exception {
    var field = Field.builder("foo").mapOf(Value.builder().intValue().required()).required();

    var doc = new BsonDocument("foo", new BsonDocument());

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field).unwrap();
      Assert.assertEquals(Map.of(), value);
    }
  }

  @Test
  public void testAsMapBuilder() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Map.ofEntries(Map.entry("foo", 1)),
            Map.ofEntries(Map.entry("bar", 2)),
            (name, doc, value) ->
                doc.append(
                    name,
                    new BsonDocument(
                        value.entrySet().stream()
                            .map(e -> new BsonElement(e.getKey(), new BsonInt32(e.getValue())))
                            .collect(Collectors.toList()))),
            name -> Field.builder(name).intField().asMap());
    test.test();
  }

  @Test
  public void mustNotBeEmptyThrowsForEmptyMap() throws Exception {
    var field =
        Field.builder("foo")
            .mapOf(Value.builder().stringValue().required())
            .mustNotBeEmpty()
            .required();

    var doc = new BsonDocument("foo", new BsonDocument());

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void mustNotContainEmptyStringAsKeyThrowsForEmptyString() throws Exception {
    var field =
        Field.builder("foo")
            .mapOf(Value.builder().stringValue().required())
            .mustNotContainEmptyStringAsKey()
            .required();

    var doc = new BsonDocument("foo", new BsonDocument("", new BsonString("bar")));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void validatesKeys() throws Exception {
    var field =
        Field.builder("foo")
            .mapOf(Value.builder().intValue().required())
            .validateKeys(
                k ->
                    k.startsWith("good")
                        ? Optional.empty()
                        : Optional.of("must start with \"good\""))
            .required();

    var goodDoc =
        new BsonDocument(
            "foo",
            new BsonDocument()
                .append("good 1", new BsonInt32(1))
                .append("good 2", new BsonInt32(2)));
    try (var parser = BsonDocumentParser.fromRoot(goodDoc).build()) {
      // Shouldn't throw.
      parser.getField(field).unwrap();
    }

    var badDoc =
        new BsonDocument(
            "foo",
            new BsonDocument()
                .append("good 1", new BsonInt32(1))
                .append("bad 2", new BsonInt32(2)));
    try (var parser = BsonDocumentParser.fromRoot(badDoc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void asOrderedMapPreservesDeclaredFieldOrder() throws Exception {
    // Keys chosen so that declared order (z, a, m, b) differs from HashMap's hash-bucket order
    // (a, b, m, z); the plain asMap() default would iterate in the latter.
    var declaredOrder = java.util.List.of("z", "a", "m", "b");
    var field = Field.builder("foo").intField().asOrderedMap().required();

    var mapDoc = new BsonDocument();
    for (int i = 0; i < declaredOrder.size(); i++) {
      mapDoc.append(declaredOrder.get(i), new BsonInt32(i));
    }
    var doc = new BsonDocument("foo", mapDoc);

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field).unwrap();
      Assert.assertEquals(declaredOrder, java.util.List.copyOf(value.keySet()));
    }
  }

  @Test
  public void asMapAndAsOrderedMapParseEqualMaps() throws Exception {
    // Ordering is the only difference between the two variants: the parsed contents are equal.
    var mapDoc = new BsonDocument().append("foo", new BsonInt32(1)).append("bar", new BsonInt32(2));
    var doc = new BsonDocument("foo", mapDoc.clone());

    var unordered = Field.builder("foo").intField().asMap().required();
    var ordered = Field.builder("foo").intField().asOrderedMap().required();

    try (var parser1 = BsonDocumentParser.fromRoot(doc.clone()).build();
        var parser2 = BsonDocumentParser.fromRoot(doc.clone()).build()) {
      Assert.assertEquals(parser1.getField(unordered).unwrap(), parser2.getField(ordered).unwrap());
    }
  }

  @Test
  public void asOrderedMapSupportsValidators() throws Exception {
    // Validator wrappers (mustNotBeEmpty et al.) preserve the ordered parse.
    var field = Field.builder("foo").intField().asOrderedMap().mustNotBeEmpty().required();

    var emptyDoc = new BsonDocument("foo", new BsonDocument());
    try (var parser = BsonDocumentParser.fromRoot(emptyDoc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }

    var doc =
        new BsonDocument(
            "foo", new BsonDocument().append("z", new BsonInt32(1)).append("a", new BsonInt32(2)));
    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field).unwrap();
      Assert.assertEquals(java.util.List.of("z", "a"), java.util.List.copyOf(value.keySet()));
    }
  }

  @Test
  public void asOrderedMapEncodesInMapOrder() throws Exception {
    // The encoder iterates the map's entrySet, so an ordered map round-trips its field order.
    var value = new java.util.LinkedHashMap<String, Integer>();
    value.put("z", 1);
    value.put("a", 2);
    value.put("m", 3);
    var field = Field.builder("foo").intField().asOrderedMap().required();

    var encoded = field.encode(value).asDocument();

    Assert.assertEquals(java.util.List.of("z", "a", "m"), java.util.List.copyOf(encoded.keySet()));
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            Map.ofEntries(Map.entry("foo", "bar"), Map.entry("baz", "qux")),
            new BsonDocument()
                .append("foo", new BsonString("bar"))
                .append("baz", new BsonString("qux")),
            name -> Field.builder(name).stringField().asMap());
    test.test();
  }
}
