package com.xgen.testing.mongot.replication.mongodb.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.status.IndexStatus.Reason;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import com.xgen.mongot.replication.mongodb.common.BufferlessIdOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.BufferlessNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.BufferlessParallelNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexStateInfo;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.PartitionResumeInfo;
import com.xgen.mongot.replication.mongodb.common.StaleStateInfo;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ValueParser;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

/**
 * Derives which {@link IndexCommitUserData} paths mongot can persist, so a reviewer can tell a
 * change that only reads a field from one that writes a field an older binary would reject. Such a
 * field costs a full resync: the older parser rejects the unknown key, {@code fromEncodedString}
 * falls back to {@link IndexCommitUserData#EMPTY}, and replication reads that as "never committed"
 * and wipes the index.
 *
 * <p>Neither derived set is both sound and precise, so there are two. {@link #deriveWritten()} is
 * precise, because empty optionals are omitted when the record is serialized, but sees only what
 * the stub arguments reach and so can miss a conditional write. {@link #deriveDeclared()} cannot
 * miss a new field, because nothing is serialized without a {@link Field} constant declaring it,
 * but also lists fields nothing populates. A new path in the written set has to be shown readable
 * by the release a rollback lands on; a new path in only the declared set is the reader-only case,
 * which is safe.
 *
 * <p>Paths are dotted, because a field added inside a strictly parsed sub-document forces the same
 * wipe as a new top-level key. A path is tracked when the document level holding it rejects unknown
 * keys, so a level declared with {@code allowUnknownFields()} contributes none of its own keys:
 * those are dropped on rollback rather than rejected. That leniency covers only the level it is
 * declared on, so the walk descends through a lenient level to reach any strictly parsed
 * sub-document beneath it -- {@code staleStateInfo} is lenient while {@code
 * staleStateInfo.changeStreamResumeInfo} is not, and a field added in there costs the same wipe as
 * a top-level one. Strictness is read from the field declarations rather than listed here, so
 * making a parser stricter cannot leave this inventory tracking the wrong paths.
 *
 * <p>{@link #deriveEnumValues()} covers a second hazard no path can show. A release binary builds
 * its enum lookup from the constants it was compiled with and rejects a value it does not know,
 * costing the same resync while leaving every path unchanged. {@code allowUnknownFields()} does not
 * help, because it tolerates unknown keys rather than unknown values, so that walk covers every
 * sub-document, including the lenient levels whose own keys the path walks leave out. Values are
 * read from the declarations only, with no written counterpart: the stub arguments pin one constant
 * per enum, so executing production code could never enumerate a domain, and any declared constant
 * is one some code path can persist.
 *
 * <p>A {@code withFallback()} declaration is reported like any other value rather than excluded. It
 * does map an unrecognized value onto a known one, but in the binary that declares it -- so it
 * earns nothing for a rollback unless the older binary carries it too. Excluding those values would
 * hide every later addition to the enum behind a guarantee that only holds if the fallback shipped
 * first, which is the reviewer's call rather than this inventory's.
 */
public final class IndexCommitUserDataFieldInventory {

  private static final String FIELDS_HOLDER_NAME = "Fields";

  /** Name of the parser library's unknown-field policy flag, read reflectively. */
  private static final String ALLOW_UNKNOWN_FIELDS_MEMBER = "allowUnknownFields";

  /** Name of the parser library's enum value lookup, keyed by the name that reaches disk. */
  private static final String ENUM_VALUES_MEMBER = "fromNames";

  /** Name of the parser library's holder for the value to fall back on, read reflectively. */
  private static final String VALUE_SEPARATOR = " = ";

  /**
   * The root document rejects unknown keys, so its keys are tracked. Unlike a nested level this
   * cannot be read off a field declaration, because no field declares the root: it is parsed by the
   * parser {@code IndexCommitUserData.fromEncodedString} builds, which takes the {@code
   * BsonDocumentParser} default rather than opting out. That default is what lets an unknown
   * top-level key empty the whole record.
   */
  private static final boolean ROOT_REJECTS_UNKNOWN_FIELDS = true;

  private static final BsonTimestamp TIMESTAMP = new BsonTimestamp(1234567890L);

  /**
   * Opaque mongod-owned document that mongot stores verbatim. Its contents stay out of the derived
   * sets because {@code resumeToken} is a plain document field rather than a strictly parsed
   * sub-document. It still has to be well formed, because {@code StaleStateInfo.create} decodes an
   * optime back out of it.
   */
  private static final BsonDocument RESUME_TOKEN = ChangeStreamUtils.resumeToken(TIMESTAMP);

  private static final ChangeStreamResumeInfo CHANGE_STREAM_RESUME_INFO =
      ChangeStreamResumeInfo.create(new MongoNamespace("database", "collection"), RESUME_TOKEN);

  /**
   * Stub arguments by parameter type. Candidates populate every optional whose path the walk
   * tracks, so a nested field stays out of the written set only when no code path can write it.
   * Listing several candidates for one type contributes all of its shapes.
   */
  private static final ImmutableMap<Class<?>, ImmutableList<Object>> STUBS =
      ImmutableMap.of(
          int.class,
          ImmutableList.of(IndexFormatVersion.CURRENT.versionNumber),
          long.class,
          ImmutableList.of(7L),
          String.class,
          ImmutableList.of("exceeded limits reason"),
          ChangeStreamResumeInfo.class,
          ImmutableList.of(CHANGE_STREAM_RESUME_INFO),
          InitialSyncResumeInfo.class,
          ImmutableList.of(
              new BufferlessIdOrderInitialSyncResumeInfo(TIMESTAMP, new BsonString("lastScanned")),
              new BufferlessNaturalOrderInitialSyncResumeInfo(
                  TIMESTAMP, RESUME_TOKEN, Optional.of("syncSourceHost")),
              new BufferlessParallelNaturalOrderInitialSyncResumeInfo(
                  TIMESTAMP,
                  UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"),
                  ImmutableList.of(
                      new PartitionResumeInfo(
                          0, Optional.of(1L), Optional.of(2L), Optional.of(3L), false)),
                  Optional.of("syncSourceHost"))),
          StaleStateInfo.class,
          ImmutableList.of(
              StaleStateInfo.create(
                  StaleStatusReason.DOCS_EXCEEDED,
                  StaleStatusReason.DOCS_EXCEEDED.formatMessage(),
                  IndexCommitUserData.createChangeStreamResume(
                      CHANGE_STREAM_RESUME_INFO, IndexFormatVersion.CURRENT.versionNumber))),
          IndexStateInfo.class,
          ImmutableList.of(
              IndexStateInfo.create(StatusCode.DOES_NOT_EXIST, Reason.COLLECTION_NOT_FOUND)),
          // The convenience shape is enough here: autoEmbeddingMetadata and every sub-document
          // beneath it are parsed with allowUnknownFields, so no path inside it is tracked. Making
          // one of those levels strict would start tracking its fields, and the assertion that
          // every declared path is written would fail until this stub populates them.
          AutoEmbeddingCommitMetadata.class,
          ImmutableList.of(new AutoEmbeddingCommitMetadata(true)));

  private IndexCommitUserDataFieldInventory() {}

  /**
   * Fails when a sealed type gains a subclass no candidate covers. Without this the new shape's
   * fields stay out of the written set while every golden assertion still passes, so the omission
   * reads as "nothing writes these" rather than "nothing constructed this shape".
   */
  private static void checkSealedStubsAreComplete() {
    for (Map.Entry<Class<?>, ImmutableList<Object>> entry : STUBS.entrySet()) {
      Class<?>[] permitted = entry.getKey().getPermittedSubclasses();
      if (permitted == null) {
        continue;
      }
      for (Class<?> subclass : permitted) {
        Check.checkState(
            entry.getValue().stream().anyMatch(subclass::isInstance),
            "No stub candidate for %s, a permitted subclass of %s. Add one to"
                + " IndexCommitUserDataFieldInventory so the fields that shape writes show up in"
                + " the written set.",
            subclass.getName(),
            entry.getKey().getName());
      }
    }
  }

  /** Returns every dotted path production code can write, sorted for stable golden comparison. */
  public static ImmutableSortedSet<String> deriveWritten() throws ReflectiveOperationException {
    checkSealedStubsAreComplete();

    List<IndexCommitUserData> constructed = new ArrayList<>();
    constructed.add(IndexCommitUserData.EMPTY);
    constructed.addAll(invokeFactories());
    constructed.addAll(invokeTransformers(constructed));

    Declarations declarations = readDeclarations(IndexCommitUserData.class);
    Set<String> paths = new TreeSet<>();
    for (IndexCommitUserData userData : constructed) {
      collectWrittenPaths(userData.toBson(), "", ROOT_REJECTS_UNKNOWN_FIELDS, declarations, paths);
    }
    return ImmutableSortedSet.copyOf(paths);
  }

  /** Returns every dotted path the schema declares, whether or not anything populates it. */
  public static ImmutableSortedSet<String> deriveDeclared() throws ReflectiveOperationException {
    return deriveDeclared(IndexCommitUserData.class);
  }

  /**
   * Returns the declared paths of {@code root}, so a test can pin how the walk treats an
   * arrangement of strict and lenient levels against a fixture. The production schema holds one
   * arrangement at a time and so cannot show that the others are handled.
   */
  public static ImmutableSortedSet<String> deriveDeclared(Class<?> root)
      throws ReflectiveOperationException {
    return readDeclarations(root).paths();
  }

  /**
   * Returns the enum values the schema accepts as {@code path = VALUE} entries, one per value, so
   * that widening an enum shows up as a single added entry.
   */
  public static ImmutableSortedSet<String> deriveEnumValues() throws ReflectiveOperationException {
    ImmutableListMultimap.Builder<String, String> valuesByPath = ImmutableListMultimap.builder();
    collectEnumValues(IndexCommitUserData.class, "", ImmutableSet.of(), valuesByPath);

    Set<String> entries = new TreeSet<>();
    valuesByPath.build().forEach((path, value) -> entries.add(path + VALUE_SEPARATOR + value));
    return ImmutableSortedSet.copyOf(entries);
  }

  /**
   * The declared paths, plus the two sets of sub-document levels the written walk needs: those that
   * reject unknown keys, whose keys it records, and those parsed as a sub-document at all, which it
   * descends into. The second is wider than the first, because a lenient level has to be descended
   * through to reach a strict one below it, and narrower than "every BSON document", because a
   * plain {@code documentField()} such as {@code resumeToken} holds opaque mongod-owned content
   * that no parser of ours reads. Both are derived alongside the paths rather than by a separate
   * traversal that could disagree with them.
   */
  private record Declarations(
      ImmutableSortedSet<String> paths,
      ImmutableSet<String> strictDocumentPaths,
      ImmutableSet<String> parsedDocumentPaths) {}

  private static Declarations readDeclarations(Class<?> root) throws ReflectiveOperationException {
    Check.checkState(
        findFieldsHolder(root).isPresent(),
        "%s declares no %s holder, so the walk would report an empty schema. Renaming that holder"
            + " would otherwise read as a schema that declares nothing, so fail here instead.",
        root.getName(),
        FIELDS_HOLDER_NAME);

    Set<String> paths = new TreeSet<>();
    Set<String> strictDocumentPaths = new TreeSet<>();
    Set<String> parsedDocumentPaths = new TreeSet<>();
    collectDeclaredPaths(
        root,
        "",
        ROOT_REJECTS_UNKNOWN_FIELDS,
        ImmutableSet.of(),
        paths,
        strictDocumentPaths,
        parsedDocumentPaths);
    return new Declarations(
        ImmutableSortedSet.copyOf(paths),
        ImmutableSet.copyOf(strictDocumentPaths),
        ImmutableSet.copyOf(parsedDocumentPaths));
  }

  private static List<IndexCommitUserData> invokeFactories() throws ReflectiveOperationException {
    List<IndexCommitUserData> constructed = new ArrayList<>();
    for (Method method : IndexCommitUserData.class.getDeclaredMethods()) {
      if (!returnsRecord(method) || !Modifier.isStatic(method.getModifiers())) {
        continue;
      }
      if (readsSerializedInput(method)) {
        continue;
      }
      for (Object[] arguments : argumentCombinations(method)) {
        constructed.add((IndexCommitUserData) method.invoke(null, arguments));
      }
    }
    return constructed;
  }

  /**
   * Applies every public instance method that returns a record to each already constructed record.
   * Deliberately not keyed on a {@code with} prefix, so a populating method that does not follow
   * the convention is still covered.
   */
  private static List<IndexCommitUserData> invokeTransformers(List<IndexCommitUserData> targets)
      throws ReflectiveOperationException {
    List<IndexCommitUserData> constructed = new ArrayList<>();
    for (Method method : IndexCommitUserData.class.getDeclaredMethods()) {
      if (!returnsRecord(method) || Modifier.isStatic(method.getModifiers())) {
        continue;
      }
      for (Object[] arguments : argumentCombinations(method)) {
        for (IndexCommitUserData target : targets) {
          constructed.add((IndexCommitUserData) method.invoke(target, arguments));
        }
      }
    }
    return constructed;
  }

  private static boolean returnsRecord(Method method) {
    return Modifier.isPublic(method.getModifiers())
        && !method.isSynthetic()
        && method.getReturnType().equals(IndexCommitUserData.class);
  }

  /**
   * Identifies parse entry points, which read a persisted record rather than producing one. Keyed
   * on the parameter type rather than the method name so the exclusion cannot quietly swallow a
   * future writer.
   */
  private static boolean readsSerializedInput(Method method) {
    for (Class<?> parameterType : method.getParameterTypes()) {
      if (parameterType.equals(DocumentParser.class)
          || parameterType.equals(EncodedUserData.class)) {
        return true;
      }
    }
    return false;
  }

  private static List<Object[]> argumentCombinations(Method method) {
    List<Object[]> combinations = new ArrayList<>();
    combinations.add(new Object[0]);
    for (Class<?> parameterType : method.getParameterTypes()) {
      ImmutableList<Object> candidates = STUBS.get(parameterType);
      Check.checkState(
          candidates != null,
          "No stub registered for parameter type %s used by %s. Add one to"
              + " IndexCommitUserDataFieldInventory so a reviewer can see which commit"
              + " metadata fields this writes.",
          parameterType.getName(),
          method.getName());

      List<Object[]> expanded = new ArrayList<>();
      for (Object[] shorter : combinations) {
        for (Object candidate : candidates) {
          Object[] arguments = new Object[shorter.length + 1];
          System.arraycopy(shorter, 0, arguments, 0, shorter.length);
          arguments[shorter.length] = candidate;
          expanded.add(arguments);
        }
      }
      combinations = expanded;
    }
    return combinations;
  }

  /**
   * Records the keys {@code document} holds, mirroring {@link #collectDeclaredPaths}: a key counts
   * only when the level holding it rejects unknown keys, and the walk descends through the lenient
   * levels anyway to reach the strict ones below them. Two assertions keep the pair in step. A key
   * this walk records where the other does not fails as a path written but not declared, and a
   * strict key it misses fails the requirement that every declared path be written or named
   * reader-only, so neither walk can drift into its own rule.
   */
  private static void collectWrittenPaths(
      BsonDocument document,
      String prefix,
      boolean ownerRejectsUnknownFields,
      Declarations declarations,
      Set<String> paths) {
    for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
      String path = extend(prefix, entry.getKey());
      if (ownerRejectsUnknownFields) {
        paths.add(path);
      }
      if (declarations.parsedDocumentPaths().contains(path)) {
        collectWrittenValue(
            entry.getValue(),
            path,
            declarations.strictDocumentPaths().contains(path),
            declarations,
            paths);
      }
    }
  }

  private static void collectWrittenValue(
      BsonValue value,
      String path,
      boolean valueRejectsUnknownFields,
      Declarations declarations,
      Set<String> paths) {
    if (value.isDocument()) {
      collectWrittenPaths(value.asDocument(), path, valueRejectsUnknownFields, declarations, paths);
    } else if (value.isArray()) {
      for (BsonValue element : value.asArray()) {
        collectWrittenValue(element, path, valueRejectsUnknownFields, declarations, paths);
      }
    }
  }

  /**
   * Walks the {@code Fields} holder of {@code owner}, descending through each field's declared
   * value type. {@code ancestors} guards against a schema that nests a type inside itself.
   *
   * <p>{@code ownerRejectsUnknownFields} says whether {@code owner} is the level an older binary
   * parses strictly, which is what decides whether its keys are recorded. Recording is kept
   * separate from descending on purpose: {@code allowUnknownFields()} is a property of the one
   * level it is declared on and is not inherited, so a lenient level still has to be walked in case
   * it parses a sub-document of its own strictly.
   */
  private static void collectDeclaredPaths(
      Class<?> owner,
      String prefix,
      boolean ownerRejectsUnknownFields,
      ImmutableSet<Class<?>> ancestors,
      Set<String> paths,
      Set<String> strictDocumentPaths,
      Set<String> parsedDocumentPaths)
      throws ReflectiveOperationException {
    if (ancestors.contains(owner)) {
      return;
    }
    for (DeclaredField field : declaredFields(owner)) {
      String path = extend(prefix, field.name());
      if (ownerRejectsUnknownFields) {
        paths.add(path);
      }

      Optional<Boolean> allowsUnknown = allowsUnknownFields(field.declared());
      Check.checkState(
          allowsUnknown.isPresent() || findFieldsHolder(field.valueType()).isEmpty(),
          "Could not read the unknown-field policy of %s from the parser library. The rollback"
              + " inventory needs it to know which sub-documents an older binary parses strictly,"
              + " so update IndexCommitUserDataFieldInventory to match the parser library.",
          path);
      if (allowsUnknown.isEmpty()) {
        continue;
      }

      parsedDocumentPaths.add(path);
      boolean fieldRejectsUnknownFields = !allowsUnknown.get();
      if (fieldRejectsUnknownFields) {
        strictDocumentPaths.add(path);
      }
      collectDeclaredPaths(
          field.valueType(),
          path,
          fieldRejectsUnknownFields,
          ImmutableSet.<Class<?>>builder().addAll(ancestors).add(owner).build(),
          paths,
          strictDocumentPaths,
          parsedDocumentPaths);
    }
  }

  /**
   * Walks the schema for enum-typed fields, descending through every sub-document rather than
   * stopping at the lenient ones, because tolerating an unknown key says nothing about tolerating
   * an unknown value.
   */
  private static void collectEnumValues(
      Class<?> owner,
      String prefix,
      ImmutableSet<Class<?>> ancestors,
      ImmutableListMultimap.Builder<String, String> valuesByPath)
      throws ReflectiveOperationException {
    if (ancestors.contains(owner)) {
      return;
    }
    for (DeclaredField field : declaredFields(owner)) {
      String path = extend(prefix, field.name());
      Optional<Object> enumParser = findEnumParser(field.declared());
      if (enumParser.isPresent()) {
        valuesByPath.putAll(path, acceptedValues(enumParser.get()));
        continue;
      }
      collectEnumValues(
          field.valueType(),
          path,
          ImmutableSet.<Class<?>>builder().addAll(ancestors).add(owner).build(),
          valuesByPath);
    }
  }

  /** The values {@code enumParser} recognizes, as they are encoded on disk. */
  private static ImmutableList<String> acceptedValues(Object enumParser)
      throws ReflectiveOperationException {
    Map<?, ?> acceptedByName =
        (Map<?, ?>) readMember(enumParser, ENUM_VALUES_MEMBER, Map.class).orElseThrow();
    ImmutableList.Builder<String> values = ImmutableList.builder();
    for (Object name : acceptedByName.keySet()) {
      values.add((String) name);
    }
    return values.build();
  }

  /** A {@code Fields} holder constant, paired with the value type it declares. */
  private record DeclaredField(Field.Nameable declared, Class<?> valueType) {
    private String name() {
      return this.declared.getName();
    }
  }

  /**
   * Reads the field constants {@code owner} declares, or nothing when it has no {@code Fields}
   * holder and so contributes no schema of its own.
   */
  private static ImmutableList<DeclaredField> declaredFields(Class<?> owner)
      throws ReflectiveOperationException {
    Optional<Class<?>> holder = findFieldsHolder(owner);
    if (holder.isEmpty()) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<DeclaredField> fields = ImmutableList.builder();
    for (java.lang.reflect.Field member : holder.get().getDeclaredFields()) {
      if (!Field.Nameable.class.isAssignableFrom(member.getType())
          || !Modifier.isStatic(member.getModifiers())) {
        continue;
      }
      member.setAccessible(true);
      fields.add(new DeclaredField((Field.Nameable) member.get(null), declaredValueType(member)));
    }
    return fields.build();
  }

  /**
   * Reads whether the parser behind {@code declared} tolerates unknown keys, or empty when the
   * value is not parsed as a sub-document and so has nothing beneath it to track. Strictness is
   * read rather than listed because a hardcoded list of strict field names would go stale the
   * moment a parser changed strictness, as CLOUDP-339757 intends for {@code staleStateInfo}.
   */
  private static Optional<Boolean> allowsUnknownFields(Field.Nameable declared)
      throws ReflectiveOperationException {
    for (Object parser : reachableParsers(declared)) {
      Optional<Object> policy = readMember(parser, ALLOW_UNKNOWN_FIELDS_MEMBER, boolean.class);
      if (policy.isPresent()) {
        return Optional.of((Boolean) policy.get());
      }
    }
    return Optional.empty();
  }

  /** The enum parser behind {@code declared}, or empty when the field is not enum typed. */
  private static Optional<Object> findEnumParser(Field.Nameable declared)
      throws ReflectiveOperationException {
    for (Object parser : reachableParsers(declared)) {
      if (readMember(parser, ENUM_VALUES_MEMBER, Map.class).isPresent()) {
        return Optional.of(parser);
      }
    }
    return Optional.empty();
  }

  /**
   * Every parser reachable from {@code declared}, nearest first. The behavior this inventory
   * depends on is held by parsers that the parser library keeps private, so it is read reflectively
   * from the declarations themselves.
   */
  private static ImmutableList<Object> reachableParsers(Field.Nameable declared)
      throws ReflectiveOperationException {
    ImmutableList.Builder<Object> parsers = ImmutableList.builder();
    Deque<Object> pending = new ArrayDeque<>();
    Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    pending.add(declared);
    while (!pending.isEmpty()) {
      Object current = pending.remove();
      if (!visited.add(current)) {
        continue;
      }
      parsers.add(current);
      for (java.lang.reflect.Field member : current.getClass().getDeclaredFields()) {
        if (Modifier.isStatic(member.getModifiers())) {
          continue;
        }
        member.setAccessible(true);
        if (member.get(current) instanceof ValueParser<?> parser) {
          pending.add(parser);
        }
      }
    }
    return parsers.build();
  }

  /**
   * Reads an instance member by name and type, or empty when {@code owner} declares no such one.
   */
  private static Optional<Object> readMember(Object owner, String name, Class<?> type)
      throws ReflectiveOperationException {
    for (java.lang.reflect.Field member : owner.getClass().getDeclaredFields()) {
      if (Modifier.isStatic(member.getModifiers())
          || !member.getName().equals(name)
          || !type.isAssignableFrom(member.getType())) {
        continue;
      }
      member.setAccessible(true);
      return Optional.ofNullable(member.get(owner));
    }
    return Optional.empty();
  }

  private static Optional<Class<?>> findFieldsHolder(Class<?> owner) {
    for (Class<?> nested : owner.getDeclaredClasses()) {
      if (nested.getSimpleName().equals(FIELDS_HOLDER_NAME)) {
        return Optional.of(nested);
      }
    }
    return Optional.empty();
  }

  /** Extracts {@code T} from a {@code Field.Optional<T>} or {@code Field.Required<T>} constant. */
  private static Class<?> declaredValueType(java.lang.reflect.Field member) {
    Type declared = member.getGenericType();
    if (!(declared instanceof ParameterizedType parameterized)) {
      return Object.class;
    }
    return resolveValueType(parameterized.getActualTypeArguments()[0]);
  }

  /**
   * Unwraps a declared value type down to the class whose fields are serialized. A collection field
   * declares its element type one level deeper, and serializes those elements under its own path.
   */
  private static Class<?> resolveValueType(Type type) {
    if (type instanceof Class<?> declared) {
      return declared;
    }
    if (type instanceof ParameterizedType parameterized
        && parameterized.getRawType() instanceof Class<?> raw) {
      return Collection.class.isAssignableFrom(raw)
          ? resolveValueType(parameterized.getActualTypeArguments()[0])
          : raw;
    }
    return Object.class;
  }

  private static String extend(String prefix, String key) {
    return prefix.isEmpty() ? key : prefix + "." + key;
  }
}
