package com.xgen.mongot.util.bson.parser;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.util.DeploymentEnvironment;
import org.junit.Test;

public class FieldTest {

  @Test
  public void required_editionNotSpecified_isEmpty() {
    var field = Field.builder("foo").stringField().required();

    assertThat(field.getEdition()).isEmpty();
  }

  @Test
  public void optional_editionNotSpecified_isEmpty() {
    var field = Field.builder("foo").stringField().optional().noDefault();

    assertThat(field.getEdition()).isEmpty();
  }

  @Test
  public void withDefault_editionNotSpecified_isEmpty() {
    var field = Field.builder("foo").stringField().optional().withDefault("bar");

    assertThat(field.getEdition()).isEmpty();
  }

  @Test
  public void required_withEdition_returnsEdition() {
    var field =
        Field.builder("foo").stringField().edition(DeploymentEnvironment.ENTERPRISE).required();

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.ENTERPRISE);
  }

  @Test
  public void optionalNoDefault_withEdition_returnsEdition() {
    var field =
        Field.builder("foo")
            .stringField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.COMMUNITY);
  }

  @Test
  public void optionalWithDefault_withEditionAfterOptional_returnsEdition() {
    var field =
        Field.builder("foo")
            .stringField()
            .optional()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .withDefault("bar");

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.ENTERPRISE);
  }

  @Test
  public void required_withEditionThenValidate_preservesEdition() {
    var field =
        Field.builder("foo")
            .stringField()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .mustNotBeEmpty()
            .required();

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.ENTERPRISE);
  }

  @Test
  public void required_withEditionThenValidateInOtherOrder_preservesEdition() {
    var field =
        Field.builder("foo")
            .stringField()
            .mustNotBeEmpty()
            .edition(DeploymentEnvironment.COMMUNITY)
            .required();

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.COMMUNITY);
  }

  @Test
  public void listRequired_withEdition_returnsEdition() {
    var field =
        Field.builder("foo")
            .stringField()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .asList()
            .required();

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.ENTERPRISE);
  }

  @Test
  public void listRequired_withEditionAfterAsList_returnsEdition() {
    var field =
        Field.builder("foo")
            .stringField()
            .asList()
            .edition(DeploymentEnvironment.COMMUNITY)
            .mustNotBeEmpty()
            .required();

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.COMMUNITY);
  }

  @Test
  public void mapRequired_withEdition_returnsEdition() {
    var field =
        Field.builder("foo")
            .stringField()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .asMap()
            .required();

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.ENTERPRISE);
  }

  @Test
  public void mapRequired_withEditionAfterAsMap_returnsEdition() {
    var field =
        Field.builder("foo")
            .stringField()
            .asMap()
            .edition(DeploymentEnvironment.COMMUNITY)
            .mustNotBeEmpty()
            .required();

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.COMMUNITY);
  }

  @Test
  public void optionalPreserveBsonNull_withEdition_preservesEdition() {
    var field =
        Field.builder("foo")
            .stringField()
            .optional()
            .edition(DeploymentEnvironment.ENTERPRISE)
            .preserveBsonNull()
            .noDefault();

    assertThat(field.getEdition()).hasValue(DeploymentEnvironment.ENTERPRISE);
  }
}
