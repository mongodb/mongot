package com.xgen.mongot.config.provider.community;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Merges CLI-supplied config values into the intermediate {@link BsonDocument} of a Community
 * config file.
 *
 * <p>Each entry of {@code cliValues} maps a dot-delimited YAML path to the value provided on the
 * command line. The value is written into the config document at that path, creating any missing
 * intermediate documents. A config value may be provided either by the YAML config file or on the
 * command line, but not both.
 */
public final class ConfigSpliceUtil {

  private ConfigSpliceUtil() {}

  /**
   * Merges CLI-supplied config values into an intermediate config {@link BsonDocument}.
   *
   * <p>Each entry of {@code cliValues} maps a dot-delimited YAML path to the value provided on the
   * command line. The value is written into {@code configDocument} at that path, creating any
   * missing intermediate documents.
   *
   * <p>A config value may be provided either by the YAML config file or on the command line, but
   * not both: if an option is set both here and in {@code configDocument}, a {@link
   * BsonParseException} is thrown.
   *
   * @param configDocument the intermediate {@link BsonDocument} parsed from the YAML config file,
   *     which is modified in place.
   * @param cliValues the CLI-supplied values keyed by dot-delimited YAML path.
   * @throws BsonParseException if a config value is set both in the config file and on the command
   *     line.
   */
  public static void splice(BsonDocument configDocument, Map<String, BsonValue> cliValues)
      throws BsonParseException {
    for (Map.Entry<String, BsonValue> cliValue : cliValues.entrySet()) {
      splicePath(configDocument, FieldPath.parse(cliValue.getKey()), cliValue.getValue());
    }
  }

  /**
   * Writes {@code value} at {@code path} in {@code document}, creating any missing intermediate
   * documents and rejecting the write when the config file already set {@code path}.
   */
  private static void splicePath(BsonDocument document, FieldPath path, BsonValue value)
      throws BsonParseException {
    @Var BsonDocument current = document;
    StringBuilder currentPath = new StringBuilder();
    for (String segment : path.getParent().map(FieldPath::getSegments).orElseGet(List::of)) {
      currentPath.append(currentPath.length() == 0 ? segment : "." + segment);
      BsonValue child = current.get(segment);
      if (child == null) {
        BsonDocument newChild = new BsonDocument();
        current.put(segment, newChild);
        current = newChild;
      } else if (child.isDocument()) {
        current = child.asDocument();
      } else {
        throw new BsonParseException(
            String.format(
                "%s is set by the config file and is not an object, so the CLI value for %s cannot"
                    + " be merged",
                currentPath, path),
            Optional.empty());
      }
    }

    String leaf = path.getLeaf();
    if (current.containsKey(leaf)) {
      throw new BsonParseException(
          "cannot be set both in the config file and on the command line", Optional.of(path));
    }
    current.put(leaf, value);
  }
}
