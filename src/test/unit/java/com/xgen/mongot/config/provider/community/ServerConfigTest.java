package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.BsonString;
import org.bson.BsonValue;
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

  private static final Field.Optional<String> SERVER_CONFIG_FIELD =
      Field.builder("server").stringField().optional().noDefault();

  /**
   * Root registry that mounts the {@link ServerConfig} registry under the {@code server} block, as
   * the Community config root does, so traversal emits the absolute YAML paths.
   */
  private static final class RootConfigTestRegistry extends CliOptionRegistry {
    final ServerConfig.ServerConfigCliOptionRegistry serverConfigCliOptionRegistry =
        new ServerConfig.ServerConfigCliOptionRegistry();

    RootConfigTestRegistry() {
      addChildRegistry(SERVER_CONFIG_FIELD, this.serverConfigCliOptionRegistry);
    }
  }

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

  @Test
  public void cliOptionRegistry_traverse_withAllOptions_returnsYamlPaths() {
    RootConfigTestRegistry registry = new RootConfigTestRegistry();
    ServerConfig.ServerConfigCliOptionRegistry server = registry.serverConfigCliOptionRegistry;
    server.serverName = Optional.of("mongot");
    server.grpcConfigCliOptionRegistry.grpcAddress = Optional.of("localhost:27027");
    server.grpcConfigCliOptionRegistry.grpcTlsCliOptionRegistry.grpcTlsMode = Optional.of("mtls");
    server.grpcConfigCliOptionRegistry.grpcTlsCliOptionRegistry.grpcTlsCertificateKeyFile =
        Optional.of(Path.of("/etc/mongot/cert.pem"));
    server
            .grpcConfigCliOptionRegistry
            .grpcTlsCliOptionRegistry
            .grpcTlsCertificateKeyFilePasswordFile =
        Optional.of(Path.of("/etc/mongot/cert-key-pass"));
    server.grpcConfigCliOptionRegistry.grpcTlsCliOptionRegistry.grpcTlsCaFile =
        Optional.of(Path.of("/etc/mongot/ca.pem"));

    Map<String, BsonValue> cliValues = registry.traverse();

    assertEquals(6, cliValues.size());
    assertEquals(new BsonString("localhost:27027"), cliValues.get("server.grpc.address"));
    assertEquals(new BsonString("mongot"), cliValues.get("server.name"));
    assertEquals(new BsonString("mtls"), cliValues.get("server.grpc.tls.mode"));
    assertEquals(
        new BsonString("/etc/mongot/cert.pem"),
        cliValues.get("server.grpc.tls.certificateKeyFile"));
    assertEquals(
        new BsonString("/etc/mongot/cert-key-pass"),
        cliValues.get("server.grpc.tls.certificateKeyFilePasswordFile"));
    assertEquals(new BsonString("/etc/mongot/ca.pem"), cliValues.get("server.grpc.tls.caFile"));
  }

  @Test
  public void cliOptionRegistry_traverse_unsetOptionsAreOmitted() {
    RootConfigTestRegistry registry = new RootConfigTestRegistry();
    registry.serverConfigCliOptionRegistry.grpcConfigCliOptionRegistry.grpcAddress =
        Optional.of("localhost:27027");

    Map<String, BsonValue> cliValues = registry.traverse();

    assertEquals(1, cliValues.size());
    assertEquals(new BsonString("localhost:27027"), cliValues.get("server.grpc.address"));
  }
}
