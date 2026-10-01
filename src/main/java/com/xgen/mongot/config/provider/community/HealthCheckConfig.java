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
import picocli.CommandLine;

public record HealthCheckConfig(String address) implements DocumentEncodable {
  private static class Fields {
    public static final Field.WithDefault<String> ADDRESS =
        Field.builder("address")
            .stringField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault("localhost:8080");
  }

  public static final class HealthCheckConfigCliOptionRegistry extends CliOptionRegistry {

    @CommandLine.Option(
        names = "--healthCheckAddress",
        description = "The address of the health check endpoint")
    public Optional<String> address = Optional.empty();

    public HealthCheckConfigCliOptionRegistry() {
      addStringLeafValue(Fields.ADDRESS, () -> this.address);
    }
  }

  public static HealthCheckConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new HealthCheckConfig(parser.getField(Fields.ADDRESS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.ADDRESS, this.address).build();
  }
}
