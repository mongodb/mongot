package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.junit.Assert;
import org.junit.Test;

public class BsonDocumentParserTest {

  @Test
  public void canParseEmptyDocumentWithNoFields() throws Exception {
    var doc = new BsonDocument();
    var parser = BsonDocumentParser.fromRoot(doc).build();
    parser.close();
  }

  @Test
  public void throwsExceptionIfUnparsedFields() {
    var doc = new BsonDocument("foo", new BsonNull());
    var parser = BsonDocumentParser.fromRoot(doc).build();

    Assert.assertThrows(BsonParseException.class, parser::close);
  }

  @Test
  public void doesNotThrowExceptionWhenAllFieldsAreParsed() throws Exception {
    var doc = new BsonDocument("foo", new BsonNull());
    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      parser.getField(Field.builder("foo").booleanField().optional().noDefault());
    }
  }

  @Test
  public void doesNotThrowExceptionIfUnparsedFieldsWithAllowUnknownFields() throws Exception {
    var doc = new BsonDocument("foo", new BsonNull());
    var parser = BsonDocumentParser.fromRoot(doc).allowUnknownFields(true).build();

    parser.close();
  }

  @Test
  public void throwsExceptionIfRequiredFieldIsMissing() throws Exception {
    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      BsonParseException e =
          Assert.assertThrows(
              BsonParseException.class,
              () -> parser.getField(Field.builder("foo").stringField().required()));

      Assert.assertEquals("\"foo\" is required", e.getMessage());
    }
  }

  @Test
  public void doesNotThrowExceptionIfOptionalFieldIsMissing() throws Exception {
    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      Assert.assertEquals(
          Optional.empty(),
          parser.getField(Field.builder("foo").stringField().optional().noDefault()).unwrap());
    }
  }

  @Test
  public void atlasRuntime_fieldWithoutEdition_isAccepted() throws Exception {
    try (var parser = parserFor(new BsonDocument(), Optional.empty())) {
      parser.getField(Field.builder("foo").stringField().optional().noDefault());
    }
  }

  @Test
  public void atlasRuntime_fieldWithEdition_throws() throws Exception {
    try (var parser = parserFor(new BsonDocument(), Optional.empty())) {
      BsonParseException e =
          Assert.assertThrows(
              BsonParseException.class,
              () ->
                  parser.getField(
                      Field.builder("foo")
                          .stringField()
                          .edition(DeploymentEnvironment.COMMUNITY)
                          .optional()
                          .noDefault()));

      Assert.assertEquals(
          "\"foo\" must not specify an edition for Atlas configurations", e.getMessage());
    }
  }

  @Test
  public void communityRuntime_fieldWithCommunityEdition_isAccepted() throws Exception {
    try (var parser = parserFor(new BsonDocument(), Optional.of(DeploymentEnvironment.COMMUNITY))) {
      parser.getField(
          Field.builder("foo")
              .stringField()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault());
    }
  }

  @Test
  public void communityRuntime_fieldWithEnterpriseEdition_throws() throws Exception {
    try (var parser = parserFor(new BsonDocument(), Optional.of(DeploymentEnvironment.COMMUNITY))) {
      BsonParseException e =
          Assert.assertThrows(
              BsonParseException.class,
              () ->
                  parser.getField(
                      Field.builder("foo")
                          .stringField()
                          .edition(DeploymentEnvironment.ENTERPRISE)
                          .optional()
                          .noDefault()));

      Assert.assertEquals("\"foo\" is not supported in the COMMUNITY edition", e.getMessage());
    }
  }

  @Test
  public void communityRuntime_fieldWithoutEdition_throws() throws Exception {
    try (var parser = parserFor(new BsonDocument(), Optional.of(DeploymentEnvironment.COMMUNITY))) {
      BsonParseException e =
          Assert.assertThrows(
              BsonParseException.class,
              () -> parser.getField(Field.builder("foo").stringField().optional().noDefault()));

      Assert.assertEquals(
          "\"foo\" must declare an edition for on-prem configurations", e.getMessage());
    }
  }

  @Test
  public void enterpriseRuntime_fieldWithCommunityEdition_isAccepted() throws Exception {
    try (var parser =
        parserFor(new BsonDocument(), Optional.of(DeploymentEnvironment.ENTERPRISE))) {
      parser.getField(
          Field.builder("foo")
              .stringField()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault());
    }
  }

  @Test
  public void enterpriseRuntime_fieldWithEnterpriseEdition_isAccepted() throws Exception {
    try (var parser =
        parserFor(new BsonDocument(), Optional.of(DeploymentEnvironment.ENTERPRISE))) {
      parser.getField(
          Field.builder("foo")
              .stringField()
              .edition(DeploymentEnvironment.ENTERPRISE)
              .optional()
              .noDefault());
    }
  }

  @Test
  public void enterpriseRuntime_fieldWithoutEdition_throws() throws Exception {
    try (var parser =
        parserFor(new BsonDocument(), Optional.of(DeploymentEnvironment.ENTERPRISE))) {
      BsonParseException e =
          Assert.assertThrows(
              BsonParseException.class,
              () -> parser.getField(Field.builder("foo").stringField().optional().noDefault()));

      Assert.assertEquals(
          "\"foo\" must declare an edition for on-prem configurations", e.getMessage());
    }
  }

  @Test
  public void hasField_fieldWithEdition_throwsUnderAtlasRuntime() throws Exception {
    try (var parser = parserFor(new BsonDocument(), Optional.empty())) {
      BsonParseException e =
          Assert.assertThrows(
              BsonParseException.class,
              () ->
                  parser.hasField(
                      Field.builder("foo")
                          .stringField()
                          .edition(DeploymentEnvironment.COMMUNITY)
                          .optional()
                          .noDefault()));

      Assert.assertEquals(
          "\"foo\" must not specify an edition for Atlas configurations", e.getMessage());
    }
  }

  @Test
  public void nestedField_inheritsRuntimeEdition() throws Exception {
    var doc = new BsonDocument("outer", new BsonDocument());
    try (var parser = parserFor(doc, Optional.of(DeploymentEnvironment.COMMUNITY))) {
      parser.getField(
          Field.builder("outer")
              .classField(NestedTagged::fromBson)
              .disallowUnknownFields()
              .edition(DeploymentEnvironment.COMMUNITY)
              .required());
    }
  }

  @Test
  public void nestedField_withoutEdition_throwsUnderOnPremRuntime() throws Exception {
    var doc = new BsonDocument("outer", new BsonDocument());
    try (var parser = parserFor(doc, Optional.of(DeploymentEnvironment.COMMUNITY))) {
      var outer =
          Field.builder("outer")
              .classField(NestedUntagged::fromBson)
              .disallowUnknownFields()
              .edition(DeploymentEnvironment.COMMUNITY)
              .required();
      BsonParseException e =
          Assert.assertThrows(BsonParseException.class, () -> parser.getField(outer));

      Assert.assertEquals(
          "\"outer.tagged\" must declare an edition for on-prem configurations", e.getMessage());
    }
  }

  @Test
  public void nestedField_withEnterpriseEdition_throwsUnderCommunityRuntime() throws Exception {
    var doc = new BsonDocument("outer", new BsonDocument());
    try (var parser = parserFor(doc, Optional.of(DeploymentEnvironment.COMMUNITY))) {
      var outer =
          Field.builder("outer")
              .classField(NestedEnterprise::fromBson)
              .disallowUnknownFields()
              .edition(DeploymentEnvironment.COMMUNITY)
              .required();
      BsonParseException e =
          Assert.assertThrows(BsonParseException.class, () -> parser.getField(outer));

      Assert.assertEquals(
          "\"outer.tagged\" is not supported in the COMMUNITY edition", e.getMessage());
    }
  }

  private static BsonDocumentParser parserFor(
      BsonDocument document, Optional<DeploymentEnvironment> deploymentEnvironment) {
    PermissiveBsonParseContext context = PermissiveBsonParseContext.root(deploymentEnvironment);
    return BsonDocumentParser.withContext(context, document).build();
  }

  private static class NestedTagged implements DocumentEncodable {

    static final Field.Optional<String> TAGGED =
        Field.builder("tagged")
            .stringField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();

    static NestedTagged fromBson(DocumentParser parser) throws BsonParseException {
      parser.getField(TAGGED);
      return new NestedTagged();
    }

    @Override
    public BsonDocument toBson() {
      return new BsonDocument();
    }
  }

  private static class NestedUntagged implements DocumentEncodable {

    static final Field.Optional<String> TAGGED =
        Field.builder("tagged").stringField().optional().noDefault();

    static NestedUntagged fromBson(DocumentParser parser) throws BsonParseException {
      parser.getField(TAGGED);
      return new NestedUntagged();
    }

    @Override
    public BsonDocument toBson() {
      return new BsonDocument();
    }
  }

  private static class NestedEnterprise implements DocumentEncodable {

    static final Field.Optional<String> TAGGED =
        Field.builder("tagged")
            .stringField()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .optional()
            .noDefault();

    static NestedEnterprise fromBson(DocumentParser parser) throws BsonParseException {
      parser.getField(TAGGED);
      return new NestedEnterprise();
    }

    @Override
    public BsonDocument toBson() {
      return new BsonDocument();
    }
  }
}
