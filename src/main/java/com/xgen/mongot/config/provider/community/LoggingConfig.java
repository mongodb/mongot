package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import org.slf4j.event.Level;
import picocli.CommandLine;

public record LoggingConfig(
    String verbosity, Optional<String> logPath, Optional<OtlpTracingConfig> otlp)
    implements DocumentEncodable {

  private static class Fields {
    public static final Field.WithDefault<String> VERBOSITY =
        Field.builder("verbosity")
            .stringField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(Level.INFO.toString());
    public static final Field.Optional<String> LOG_PATH =
        Field.builder("logPath")
            .stringField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();
    public static final Field.Optional<OtlpTracingConfig> OTLP =
        Field.builder("otlp")
            .classField(OtlpTracingConfig::fromBson)
            .disallowUnknownFields()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();
  }

  /** Convenience for callers that do not configure an OTLP log sink. */
  public LoggingConfig(String verbosity, Optional<String> logPath) {
    this(verbosity, logPath, Optional.empty());
  }

  public static final class LoggingConfigCliOptionRegistry extends CliOptionRegistry {

    @CommandLine.Option(
        names = "--loggingVerbosity",
        description = "Verbosity of logging, the value must be a valid Logback log level")
    public Optional<String> verbosity = Optional.empty();

    @CommandLine.Option(names = "--logPath", description = "File path where mongot writes logs.")
    public Optional<String> logPath = Optional.empty();

    public LoggingConfigCliOptionRegistry() {
      addStringLeafValue(Fields.VERBOSITY, () -> this.verbosity);
      addStringLeafValue(Fields.LOG_PATH, () -> this.logPath);
    }
  }

  public static LoggingConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new LoggingConfig(
        parser.getField(Fields.VERBOSITY).unwrap(),
        parser.getField(Fields.LOG_PATH).unwrap(),
        parser.getField(Fields.OTLP).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.VERBOSITY, this.verbosity)
        .field(Fields.LOG_PATH, this.logPath)
        .field(Fields.OTLP, this.otlp)
        .build();
  }
}
