package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServerConfigTest {

  @Mock BsonParseContext context;
  RandomStringUtils stringUtils = RandomStringUtils.insecure();

  @Before
  public void setup() throws BsonParseException {
    doThrow(new BsonParseException("Exception", Optional.empty()))
        .when(this.context)
        .handleSemanticError(anyString());
  }

  @Test()
  public void testValidateServerName_ValidAlphanumeric_NoError() throws Exception {
    ServerConfig.validateServerName(this.context, this.stringUtils.nextAlphanumeric(50));
    verify(this.context, never()).handleSemanticError(anyString());
  }

  @Test
  public void testValidateServerName_ValidPeriodHyphensAndUnderscores_NoError() throws Exception {
    Set<String> supportedChars = Set.of("-", "_", ".");
    for (String character : supportedChars) {
      ServerConfig.validateServerName(
          this.context,
          this.stringUtils.nextAlphanumeric(50)
              + character
              + this.stringUtils.nextAlphanumeric(50));
      verify(this.context, never()).handleSemanticError(anyString());
    }
  }

  @Test
  public void testValidateServerName_ValidMixedCharacters_NoError() throws Exception {
    ServerConfig.validateServerName(this.context, "Server-1_test.name");
    verify(this.context, never()).handleSemanticError(anyString());
  }

  @Test
  public void testValidateServerName_ValidSingleCharacter_NoError() throws Exception {
    ServerConfig.validateServerName(this.context, this.stringUtils.nextAlphanumeric(1));
    verify(this.context, never()).handleSemanticError(anyString());
  }

  // 253 is the inclusive upper bound: a name of exactly 253 characters is valid, and only
  // longer names are rejected.
  @Test
  public void testValidateServerName_ValidMaxLength253_NoError() throws Exception {
    String maxLengthName = this.stringUtils.nextAlphanumeric(253);
    ServerConfig.validateServerName(this.context, maxLengthName);
    verify(this.context, never()).handleSemanticError(anyString());
  }

  @Test
  public void testValidateServerName_ValidComplexName_NoError() throws Exception {
    ServerConfig.validateServerName(this.context, "server-1_test.name-2_prod.env");
    verify(this.context, never()).handleSemanticError(anyString());
  }

  @Test
  public void testValidateServerName_Null_ThrowsException() throws Exception {
    assertThrows(
        BsonParseException.class, () -> ServerConfig.validateServerName(this.context, null));
    verify(this.context).handleSemanticError("server name must not be blank");
  }

  @Test
  public void testValidateServerName_EmptyString_ThrowsException() throws Exception {
    assertThrows(BsonParseException.class, () -> ServerConfig.validateServerName(this.context, ""));
    verify(this.context).handleSemanticError("server name must not be blank");
  }

  @Test
  public void testValidateServerName_WhitespaceOnly_ThrowsException() throws Exception {
    assertThrows(
        BsonParseException.class, () -> ServerConfig.validateServerName(this.context, "   "));
    verify(this.context).handleSemanticError("server name must not be blank");
  }

  @Test
  public void testValidateServerName_TabsAndSpaces_ThrowsException() throws Exception {
    assertThrows(
        BsonParseException.class, () -> ServerConfig.validateServerName(this.context, "\t  \n  "));
    verify(this.context).handleSemanticError("server name must not be blank");
  }

  @Test
  public void testValidateServerName_ExceedsMaxLength_ThrowsException() throws Exception {
    String tooLongName = this.stringUtils.nextAlphanumeric(254);
    assertThrows(
        BsonParseException.class, () -> ServerConfig.validateServerName(this.context, tooLongName));
    verify(this.context).handleSemanticError("server name must be at most 253 characters");
  }

  @Test
  public void testValidateServerName_ContainsSpace_ThrowsException() throws Exception {
    Set<String> invalidChars =
        Set.of(
            " ", "!", "@", "#", "$", "%", "&", "*", "(", ")", "[", "]", "{", "}", "/", "\\", ":",
            ";", "\"", "'", "<", ">", "?", "~", "`", "|", "^");

    for (String character : invalidChars) {
      Mockito.clearInvocations(this.context);
      assertThrows(
          BsonParseException.class,
          () ->
              ServerConfig.validateServerName(
                  this.context,
                  this.stringUtils.nextAlphanumeric(50)
                      + character
                      + this.stringUtils.nextAlphanumeric(50)));
      verify(this.context)
          .handleSemanticError(
              "server name must only contain alphanumeric characters, periods, hyphens "
                  + "and underscores");
    }
  }
}
