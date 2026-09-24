package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;
import picocli.CommandLine;

public record StorageConfig(Path dataPath) implements DocumentEncodable {
  private static class Fields {
    public static final Field.Required<Path> DATA_PATH =
        Field.builder("dataPath").classField(PathField.PARSER, PathField.ENCODER).required();
  }

  public static final class StorageConfigCliOptionRegistry extends CliOptionRegistry {

    @CommandLine.Option(
        names = "--storageDataPath",
        description =
            "Base path for used by Mongot for storing index data and the local index catalog.")
    public Optional<Path> dataPath = Optional.empty();

    public StorageConfigCliOptionRegistry() {
      addPathLeafValue(Fields.DATA_PATH, () -> this.dataPath);
    }
  }

  public static StorageConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new StorageConfig(parser.getField(Fields.DATA_PATH).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.DATA_PATH, this.dataPath).build();
  }
}
