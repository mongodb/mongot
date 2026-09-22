package com.xgen.mongot.config.provider.cli;

import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Base class for a registry that maps command-line options onto the YAML fields of a mongot config
 * file.
 *
 * <p>Each .*Config class gets its own {@code *CliOptionRegistry} subclass, colocated with its
 * Fields. A subclass declares one {@code @CommandLine.Option} field per configurable scalar YAML
 * field, then registers those options in its constructor through the {@code addXxxLeafValue}
 * helpers and any nested blocks through {@link #addChildRegistry}.
 *
 * <p>Together, the registries form a tree that mirrors the config structure: a registry node
 * corresponds to a config block, lists the blocks nested below it in {@link #childRegistries}, and
 * exposes each configurable scalar YAML field of its block as an entry in {@link #leafCliValues}.
 *
 * <p>A CliOptionRegistry does not store its YAML path. Instead, each {@link #addChildRegistry child
 * edge} carries the name of the config field whose block the child mirrors, and {@link #traverse()}
 * derives a node's absolute YAML path by joining those field names from the root down.
 *
 * <p>A leaf is registered by keying it to its field's YAML name. Each helper takes a {@link
 * Supplier} of the option value (an {@link Optional} for scalar options), so the value is read
 * lazily when {@link #traverse()} runs — after PicoCli has injected the option fields.
 *
 * <p>{@link #traverse()} walks the tree and returns the specified values keyed by their absolute
 * (dot-delimited) YAML path. Callers can then apply those values onto the target config's BSON
 * document.
 */
public abstract class CliOptionRegistry {

  /** The branch nodes below this node, keyed by the name of the YAML block they mirror. */
  private final Map<String, CliOptionRegistry> childRegistries = new LinkedHashMap<>();

  /**
   * Maps each YAML field name to a supplier of the corresponding CLI value, relative to this node.
   * The supplier returns an {@link Optional#empty() empty Optional} when its CLI option was not
   * specified.
   */
  private final Map<String, Supplier<Optional<BsonValue>>> leafCliValues = new LinkedHashMap<>();

  /**
   * Registers {@code child} as the registry for the config block stored under {@code field}.
   *
   * <p>The YAML path this child emits is derived from {@code field}'s name: traversal prefixes the
   * field names of the edges on the path from the root to this child onto the child's own leaf
   * field names.
   *
   * @param field the config field of this node's block that {@code child} mirrors.
   * @param child the registry for the child config block.
   */
  protected final void addChildRegistry(Field.Nameable field, CliOptionRegistry child) {
    this.childRegistries.put(field.getName(), child);
  }

  /**
   * Registers {@code value} as a string {@link BsonValue} leaf for {@code field}.
   *
   * <p>The leaf is keyed by {@code field}'s YAML name, resolved to an absolute path at {@link
   * #traverse()} time from the field names of the child edges on the path from the root. It is
   * emitted only when {@code value} is present, producing a {@link BsonString}. When the CLI string
   * option was not provided, {@code value} is empty and no leaf is emitted.
   *
   * @param field the config field the CLI value sets.
   * @param value the supplier of the CLI-provided string value.
   */
  protected final void addStringLeafValue(Field.Nameable field, Supplier<Optional<String>> value) {
    this.leafCliValues.put(field.getName(), () -> value.get().map(BsonString::new));
  }

  /**
   * Registers {@code path} as a string {@link BsonValue} leaf for {@code field}.
   *
   * <p>The leaf is keyed by {@code field}'s YAML name, resolved to an absolute path at {@link
   * #traverse()} time from the field names of the child edges on the path from the root. It is
   * emitted only when {@code path} is present, producing a {@link BsonString} of the path string.
   * When the CLI path option was not provided, {@code path} is empty and no leaf is emitted.
   *
   * @param field the config field the CLI value sets.
   * @param value the supplier of the CLI-provided path value.
   */
  protected final void addPathLeafValue(Field.Nameable field, Supplier<Optional<Path>> value) {
    this.leafCliValues.put(
        field.getName(), () -> value.get().map(path -> new BsonString(path.toString())));
  }

  /**
   * Registers {@code value} as a boolean {@link BsonValue} leaf for {@code field}.
   *
   * <p>The leaf is keyed by {@code field}'s YAML name, resolved to an absolute path at {@link
   * #traverse()} time from the field names of the child edges on the path from the root. It is
   * emitted only when {@code value} is present.
   *
   * @param field the config field the CLI value sets.
   * @param value the supplier of the CLI-provided boolean value.
   */
  protected final void addBooleanLeafValue(
      Field.Nameable field, Supplier<Optional<Boolean>> value) {
    this.leafCliValues.put(field.getName(), () -> value.get().map(BsonBoolean::valueOf));
  }

  /**
   * Registers {@code values} as an array of string {@link BsonValue}s for {@code field}.
   *
   * <p>The leaf is keyed by {@code field}'s YAML name, resolved to an absolute path at {@link
   * #traverse()} time from the field names of the child edges on the path from the root. It is
   * emitted only when {@code values} is non-empty.
   *
   * @param field the config field the CLI values set.
   * @param values the supplier of the CLI-provided string values.
   */
  protected final void addStringListLeafValue(Field.Nameable field, Supplier<List<String>> values) {
    this.leafCliValues.put(
        field.getName(),
        () ->
            Optional.ofNullable(values.get())
                .filter(list -> !list.isEmpty())
                .map(list -> new BsonArray(list.stream().map(BsonString::new).toList())));
  }

  /**
   * Registers {@code jsonArray} as an array {@link BsonValue} leaf for {@code field}.
   *
   * <p>The leaf is keyed by {@code field}'s YAML name, resolved to an absolute path at {@link
   * #traverse()} time from the field names of the child edges on the path from the root. It is
   * emitted only when {@code jsonArray} is present. The JSON array string is parsed as a {@link
   * BsonArray}.
   *
   * @param field the config field the CLI value sets.
   * @param value the supplier of the CLI-provided JSON array string.
   */
  protected final void addJsonArrayLeafValue(
      Field.Nameable field, Supplier<Optional<String>> value) {
    this.leafCliValues.put(field.getName(), () -> value.get().map(BsonArray::parse));
  }

  /**
   * Registers {@code value} as an integer {@link BsonValue} leaf for {@code field}.
   *
   * <p>The leaf is keyed by {@code field}'s YAML name, resolved to an absolute path at {@link
   * #traverse()} time from the field names of the child edges on the path from the root. It is
   * emitted only when {@code value} is present.
   *
   * @param field the config field the CLI value sets.
   * @param value the supplier of the CLI-provided integer value.
   */
  protected final void addIntegerLeafValue(
      Field.Nameable field, Supplier<Optional<Integer>> value) {
    this.leafCliValues.put(field.getName(), () -> value.get().map(BsonInt32::new));
  }

  /**
   * Registers {@code value} as a double {@link BsonValue} leaf for {@code field}.
   *
   * <p>The leaf is keyed by {@code field}'s YAML name, resolved to an absolute path at {@link
   * #traverse()} time from the field names of the child edges on the path from the root. It is
   * emitted only when {@code value} is present.
   *
   * @param field the config field the CLI value sets.
   * @param value the supplier of the CLI-provided double value.
   */
  protected final void addDoubleLeafValue(Field.Nameable field, Supplier<Optional<Double>> value) {
    this.leafCliValues.put(field.getName(), () -> value.get().map(BsonDouble::new));
  }

  /**
   * Returns the CLI values specified by this registry tree, keyed by their absolute (dot-delimited)
   * YAML path.
   *
   * <p>The values are read from the option {@link Supplier}s at the time this method is called, so
   * they reflect options that were populated since the registry was constructed — for example, by
   * PicoCli, after it has injected this command object's options.
   *
   * @return the specified CLI values keyed by their absolute YAML path. A leaf is omitted when its
   *     option was not specified.
   */
  public Map<String, BsonValue> traverse() {
    return traverse("");
  }

  /** Returns this node's CLI values with {@code pathPrefix} prepended to every emitted key. */
  private Map<String, BsonValue> traverse(String pathPrefix) {
    Map<String, BsonValue> cliValues = new LinkedHashMap<>();
    for (Map.Entry<String, Supplier<Optional<BsonValue>>> leaf : this.leafCliValues.entrySet()) {
      leaf.getValue()
          .get()
          .ifPresent(value -> cliValues.put(fullPath(pathPrefix, leaf.getKey()), value));
    }
    for (Map.Entry<String, CliOptionRegistry> child : this.childRegistries.entrySet()) {
      cliValues.putAll(child.getValue().traverse(fullPath(pathPrefix, child.getKey())));
    }
    return cliValues;
  }

  /**
   * Joins {@code pathPrefix} and {@code segment}, or returns {@code segment} if the prefix is
   * empty.
   */
  private static String fullPath(String pathPrefix, String segment) {
    return pathPrefix.isEmpty() ? segment : pathPrefix + "." + segment;
  }
}
