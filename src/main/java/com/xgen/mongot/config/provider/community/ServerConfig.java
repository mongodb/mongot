package com.xgen.mongot.config.provider.community;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.config.util.TlsMode;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import picocli.CommandLine;

public record ServerConfig(GrpcServerConfig grpc, Optional<String> name)
    implements DocumentEncodable {

  private static final Pattern SERVER_NAME_VALID_REGEX = Pattern.compile("^[a-zA-Z0-9._-]*$");

  private static class Fields {
    public static final Field.Required<GrpcServerConfig> GRPC =
        Field.builder("grpc")
            .classField(GrpcServerConfig::fromBson)
            .disallowUnknownFields()
            .required();

    public static final Field.Optional<String> NAME =
        Field.builder("name").stringField().optional().noDefault();
  }

  public static final class ServerConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Option(
        names = "--serverName",
        description = "User-provided name to identify a mongot instance")
    public Optional<String> serverName = Optional.empty();

    @CommandLine.Mixin
    public GrpcServerConfig.GrpcServerConfigCliOptionRegistry grpcConfigCliOptionRegistry =
        new GrpcServerConfig.GrpcServerConfigCliOptionRegistry();

    public ServerConfigCliOptionRegistry() {
      addChildRegistry(Fields.GRPC, this.grpcConfigCliOptionRegistry);
      addStringLeafValue(Fields.NAME, () -> this.serverName);
    }
  }

  public static ServerConfig fromBson(DocumentParser parser) throws BsonParseException {
    ServerConfig config =
        new ServerConfig(
            parser.getField(Fields.GRPC).unwrap(), parser.getField(Fields.NAME).unwrap());

    if (config.name().isPresent()) {
      validateServerName(parser.getContext(), config.name().get());
    }

    return config;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.GRPC, this.grpc)
        .field(Fields.NAME, this.name)
        .build();
  }

  public TlsMode getGrpcTlsMode() {
    return this.grpc.tls().map(GrpcServerConfig.GrpcTls::mode).orElse(TlsMode.DISABLED);
  }

  @VisibleForTesting
  protected static void validateServerName(BsonParseContext context, String serverName)
      throws BsonParseException {
    if (StringUtils.isBlank(serverName)) {
      context.handleSemanticError("server name must not be blank");
    }

    if (serverName.length() > 253) {
      context.handleSemanticError("server name must be at most 253 characters");
    }

    if (!SERVER_NAME_VALID_REGEX.matcher(serverName).matches()) {
      context.handleSemanticError(
          "server name must only contain alphanumeric characters, periods, hyphens"
              + " and underscores");
    }
  }

  public record GrpcServerConfig(String address, Optional<GrpcServerConfig.GrpcTls> tls)
      implements DocumentEncodable {
    private static class Fields {
      public static final Field.Required<String> ADDRESS =
          Field.builder("address").stringField().required();
      public static final Field.Optional<GrpcServerConfig.GrpcTls> TLS =
          Field.builder("tls")
              .classField(GrpcServerConfig.GrpcTls::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();
    }

    public record GrpcTls(
        TlsMode mode,
        Optional<Path> certificateKeyFile,
        Optional<Path> certificateKeyFilePasswordFile,
        Optional<Path> caFile)
        implements DocumentEncodable {
      private static class Fields {
        public static final Field.Required<TlsMode> MODE =
            Field.builder("mode").enumField(TlsMode.class).asCaseInsensitive().required();
        public static final Field.Optional<Path> CERTIFICATE_KEY_FILE =
            Field.builder("certificateKeyFile")
                .classField(PathField.PARSER, PathField.ENCODER)
                .optional()
                .noDefault();
        public static final Field.Optional<Path> CERTIFICATE_AUTHORITY_FILE =
            Field.builder("caFile")
                .classField(PathField.PARSER, PathField.ENCODER)
                .optional()
                .noDefault();
        public static final Field.Optional<Path> CERTIFICATE_KEY_FILE_PASSWORD_FILE =
            Field.builder("certificateKeyFilePasswordFile")
                .classField(PathField.PARSER, PathField.ENCODER)
                .optional()
                .noDefault();
      }

      public static final class GrpcTlsCliOptionRegistry extends CliOptionRegistry {
        @CommandLine.Option(
            names = "--serverGrpcTlsMode",
            description = "The TLS mode for the gRPC server: TLS, mTLS, or disabled")
        public Optional<String> grpcTlsMode = Optional.empty();

        @CommandLine.Option(
            names = "--serverGrpcTlsCertificateKeyFile",
            description = "PEM file that contains a valid X.509 certificate for mongot")
        public Optional<Path> grpcTlsCertificateKeyFile = Optional.empty();

        @CommandLine.Option(
            names = "--serverGrpcTlsCertificateKeyFilePasswordFile",
            description =
                "Path to the file that contains the password to decrypt the certificate key file "
                    + "specified in serverGrpcTlsCertificateKeyFile")
        public Optional<Path> grpcTlsCertificateKeyFilePasswordFile = Optional.empty();

        @CommandLine.Option(
            names = "--serverGrpcTlsCaFile",
            description =
                "Certificate Authority (CA) file that contains trusted certificates for verifying "
                    + "the certificate presented by mongod")
        public Optional<Path> grpcTlsCaFile = Optional.empty();

        public GrpcTlsCliOptionRegistry() {
          addStringLeafValue(GrpcTls.Fields.MODE, () -> this.grpcTlsMode);
          addPathLeafValue(
              GrpcTls.Fields.CERTIFICATE_KEY_FILE, () -> this.grpcTlsCertificateKeyFile);
          addPathLeafValue(
              GrpcTls.Fields.CERTIFICATE_KEY_FILE_PASSWORD_FILE,
              () -> this.grpcTlsCertificateKeyFilePasswordFile);
          addPathLeafValue(GrpcTls.Fields.CERTIFICATE_AUTHORITY_FILE, () -> this.grpcTlsCaFile);
        }
      }

      public static GrpcServerConfig.GrpcTls fromBson(DocumentParser parser)
          throws BsonParseException {
        TlsMode mode = parser.getField(GrpcServerConfig.GrpcTls.Fields.MODE).unwrap();
        Optional<Path> certificateKeyFile = parser.getField(Fields.CERTIFICATE_KEY_FILE).unwrap();
        Optional<Path> certificateKeyFilePasswordFile =
            parser.getField(Fields.CERTIFICATE_KEY_FILE_PASSWORD_FILE).unwrap();
        Optional<Path> caFile = parser.getField(Fields.CERTIFICATE_AUTHORITY_FILE).unwrap();
        if (mode != TlsMode.DISABLED && certificateKeyFile.isEmpty()) {
          parser
              .getContext()
              .handleSemanticError("certificateKeyFile is required when tls is enabled");
        }
        if (mode == TlsMode.MTLS && caFile.isEmpty()) {
          parser.getContext().handleSemanticError("caFile is required when mtls is enabled");
        }
        if (certificateKeyFilePasswordFile.isPresent() && certificateKeyFile.isEmpty()) {
          parser
              .getContext()
              .handleSemanticError(
                  "certificateKeyFile required when certificateKeyFilePasswordFile is set");
        }
        return new GrpcServerConfig.GrpcTls(
            mode, certificateKeyFile, certificateKeyFilePasswordFile, caFile);
      }

      @Override
      public BsonDocument toBson() {
        return BsonDocumentBuilder.builder()
            .field(Fields.MODE, this.mode)
            .field(Fields.CERTIFICATE_KEY_FILE, this.certificateKeyFile)
            .field(Fields.CERTIFICATE_AUTHORITY_FILE, this.caFile)
            .field(Fields.CERTIFICATE_KEY_FILE_PASSWORD_FILE, this.certificateKeyFilePasswordFile)
            .build();
      }
    }

    public static GrpcServerConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new GrpcServerConfig(
          parser.getField(Fields.ADDRESS).unwrap(), parser.getField(Fields.TLS).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.ADDRESS, this.address)
          .field(Fields.TLS, this.tls)
          .build();
    }

    public static final class GrpcServerConfigCliOptionRegistry extends CliOptionRegistry {

      @CommandLine.Option(
          names = "--serverGrpcAddress",
          description = "Address on which the gRPC listen server listens")
      public Optional<String> grpcAddress = Optional.empty();

      @CommandLine.Mixin
      public GrpcTls.GrpcTlsCliOptionRegistry grpcTlsCliOptionRegistry =
          new GrpcTls.GrpcTlsCliOptionRegistry();

      public GrpcServerConfigCliOptionRegistry() {
        addChildRegistry(GrpcServerConfig.Fields.TLS, this.grpcTlsCliOptionRegistry);
        addStringLeafValue(GrpcServerConfig.Fields.ADDRESS, () -> this.grpcAddress);
      }
    }
  }

  public Optional<Path> getGrpcCertificateKeyFile() {
    return this.grpc.tls().flatMap(GrpcServerConfig.GrpcTls::certificateKeyFile);
  }

  public Optional<Path> getGrpcCertificateKeyFilePasswordFile() {
    return this.grpc.tls().flatMap(GrpcServerConfig.GrpcTls::certificateKeyFilePasswordFile);
  }

  public Optional<Path> getGrpcCaFile() {
    return this.grpc.tls().flatMap(GrpcServerConfig.GrpcTls::caFile);
  }
}
