package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.testing.mongot.replication.mongodb.common.IndexCommitUserDataFieldInventory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;
import org.junit.Test;

/**
 * Locks down which {@link IndexCommitUserData} fields exist, which production code can write, and
 * which enum values it accepts, so a reviewer can tell a change that only reads a field from one
 * that writes something an older binary would reject and resync away.
 *
 * <p>Every golden file holds generated output, not declarations. A change cannot assert that a
 * field goes unwritten, only leave the code such that nothing writes it, so a diff in the written
 * set has to be cleared against the release a rollback lands on, as {@code
 * docs/internal/development/commit-metadata-rollback-policy.md} describes. A diff in only the
 * declared set is the reader-only case, which is safe. The enum values are a separate hazard with
 * the same cost, because an older binary rejects a value it was not compiled with whether or not
 * the surrounding parser tolerates unknown keys.
 */
public class IndexCommitUserDataFieldInventoryTest {

  /**
   * Paths the schema parses but nothing writes. A stub that stops building the maximal shape looks
   * exactly like a genuine reader-only field in the goldens, so the reader-only ones are listed
   * here and everything else has to be written.
   */
  private static final ImmutableSet<String> READER_ONLY_PATHS =
      ImmutableSet.of(
          // Buffered initial sync shapes, deprecated and parsed only so a record written before
          // bufferless initial sync still loads.
          "initialSyncResumeInfo.changeStreamApplication", "initialSyncResumeInfo.collectionScan");

  private static final Path RESOURCE_DIRECTORY =
      Path.of("src/test/unit/resources/replication/mongodb/common");
  private static final Path WRITTEN_GOLDEN_FILE =
      RESOURCE_DIRECTORY.resolve("index-commit-user-data-written-fields.txt");
  private static final Path DECLARED_GOLDEN_FILE =
      RESOURCE_DIRECTORY.resolve("index-commit-user-data-declared-fields.txt");
  private static final Path ENUM_VALUES_GOLDEN_FILE =
      RESOURCE_DIRECTORY.resolve("index-commit-user-data-enum-values.txt");

  @Test
  public void deriveWritten_pathsProductionWrites_matchGoldenFile() throws Exception {
    ImmutableSortedSet<String> writtenPaths = IndexCommitUserDataFieldInventory.deriveWritten();
    ImmutableList<String> goldenPaths = readGoldenFile(WRITTEN_GOLDEN_FILE);

    assertWithMessage(describeWrittenDiff(writtenPaths))
        .that(writtenPaths)
        .containsExactlyElementsIn(goldenPaths);
  }

  @Test
  public void deriveDeclared_pathsSchemaDeclares_matchGoldenFile() throws Exception {
    ImmutableSortedSet<String> declaredPaths = IndexCommitUserDataFieldInventory.deriveDeclared();
    ImmutableList<String> goldenPaths = readGoldenFile(DECLARED_GOLDEN_FILE);

    assertWithMessage(describeDeclaredDiff(declaredPaths))
        .that(declaredPaths)
        .containsExactlyElementsIn(goldenPaths);
  }

  @Test
  public void deriveEnumValues_valuesSchemaAccepts_matchGoldenFile() throws Exception {
    ImmutableSortedSet<String> enumValues = IndexCommitUserDataFieldInventory.deriveEnumValues();
    ImmutableList<String> goldenValues = readGoldenFile(ENUM_VALUES_GOLDEN_FILE);

    assertWithMessage(describeEnumValuesDiff(enumValues))
        .that(enumValues)
        .containsExactlyElementsIn(goldenValues);
  }

  /**
   * The declared set backstops a write the written set cannot see, such as a factory that populates
   * a field only for some arguments. That only holds while it really is a superset, so the
   * invariant is checked rather than assumed.
   */
  @Test
  public void deriveDeclared_everyWrittenPath_isAlsoDeclared() throws Exception {
    ImmutableSortedSet<String> declaredPaths = IndexCommitUserDataFieldInventory.deriveDeclared();
    ImmutableSortedSet<String> writtenPaths = IndexCommitUserDataFieldInventory.deriveWritten();

    assertWithMessage(
            "A path is written but not declared, so the declared set has a blind spot and can no"
                + " longer backstop the written set. Teach IndexCommitUserDataFieldInventory how to"
                + " reach the missing declaration rather than editing a golden file.")
        .that(declaredPaths)
        .containsAtLeastElementsIn(writtenPaths);
  }

  /**
   * Keeps the written set maximal. The stub arguments in IndexCommitUserDataFieldInventory decide
   * which fields get populated, and a feature flag never runs during the derivation, so a stub that
   * leaves an optional empty would report the field as unwritten and silently exempt it from the
   * rollback question. Requiring every declared path to be written or named here turns that into a
   * build failure.
   */
  @Test
  public void deriveWritten_everyDeclaredPath_isWrittenOrKnownReaderOnly() throws Exception {
    ImmutableSortedSet<String> declaredPaths = IndexCommitUserDataFieldInventory.deriveDeclared();
    ImmutableSortedSet<String> writtenPaths = IndexCommitUserDataFieldInventory.deriveWritten();

    assertWithMessage(
            "A declared path is neither written nor listed in READER_ONLY_PATHS. Populate it from a"
                + " stub in IndexCommitUserDataFieldInventory so the written set keeps describing"
                + " the maximal record, and only add it to READER_ONLY_PATHS if production"
                + " genuinely cannot write it.")
        .that(Sets.union(writtenPaths, READER_ONLY_PATHS))
        .containsAtLeastElementsIn(declaredPaths);

    assertWithMessage(
            "A path in READER_ONLY_PATHS is written after all, so the exemption is hiding a real"
                + " write. Remove it from READER_ONLY_PATHS.")
        .that(Sets.intersection(READER_ONLY_PATHS, writtenPaths))
        .isEmpty();

    assertWithMessage(
            "A path in READER_ONLY_PATHS is no longer declared, so the exemption is stale and would"
                + " silently cover a future field of the same name. Remove it.")
        .that(declaredPaths)
        .containsAtLeastElementsIn(READER_ONLY_PATHS);
  }

  /**
   * Pins which paths the walks track: a key counts when the level holding it rejects unknown keys,
   * and a lenient level is descended through rather than stopped at, because {@code
   * allowUnknownFields()} covers the level it is declared on and not a sub-document that level
   * parses strictly.
   *
   * <p>{@code lenient.strict} is expected to be absent for the same reason {@code
   * lenient.tolerated} is: both are keys of the lenient level, so adding either on top of a release
   * that knows neither is safe. What has to hold is that the strict level {@code lenient.strict}
   * introduces is still walked, which {@code lenient.strict.rejected} is there to show.
   *
   * <p>Checked against a fixture because the production schema cannot show it. Its one strict level
   * under a lenient parent, {@code staleStateInfo.changeStreamResumeInfo}, parses a class the root
   * also reaches strictly, so a field added in there shows up under that other path either way. A
   * schema without a second route would leave all three goldens unchanged while still emptying
   * every index on rollback, which is what this test exists to keep impossible.
   */
  @Test
  public void deriveDeclared_pathsUnderLenientLevel_trackOnlyStrictOnes() throws Exception {
    ImmutableSortedSet<String> declaredPaths =
        IndexCommitUserDataFieldInventory.deriveDeclared(FixtureRoot.class);

    assertWithMessage(
            "The walk no longer tracks exactly the paths an older binary would reject. A key of a"
                + " lenient level is dropped on rollback and so is not tracked; a key of a strict"
                + " level empties the whole record and so is, however deep under a lenient level it"
                + " sits.")
        .that(declaredPaths)
        .containsExactly("topLevel", "lenient", "lenient.strict.rejected");
  }

  /**
   * Explains the consequences of a diff and prints the derived set ready to paste as the golden.
   */
  private static String describeWrittenDiff(ImmutableSortedSet<String> writtenPaths) {
    return "The set of commit metadata fields mongot writes no longer matches "
        + WRITTEN_GOLDEN_FILE
        + ". A field a release binary cannot parse costs a full resync on rollback (CLOUDP-411951),"
        + " so update this file only once the release a rollback lands on is known to declare the"
        + " new path already; see docs/internal/development/commit-metadata-rollback-policy.md. A"
        + " change that only reads a field leaves this set alone. Derived set:\n"
        + String.join("\n", writtenPaths);
  }

  private static String describeDeclaredDiff(ImmutableSortedSet<String> declaredPaths) {
    return "The set of commit metadata fields the schema declares no longer matches "
        + DECLARED_GOLDEN_FILE
        + ". A new path here with no matching change to "
        + WRITTEN_GOLDEN_FILE
        + " is the reader-only case, which is safe on rollback because empty optionals are omitted"
        + " when the record is serialized. Claim that case by adding the path to READER_ONLY_PATHS,"
        + " which is asserted separately, so a field production can in fact write cannot pass as"
        + " reader-only (CLOUDP-411951). See"
        + " docs/internal/development/commit-metadata-rollback-policy.md. Derived set:\n"
        + String.join("\n", declaredPaths);
  }

  private static String describeEnumValuesDiff(ImmutableSortedSet<String> enumValues) {
    return "The set of commit metadata enum values mongot accepts no longer matches "
        + ENUM_VALUES_GOLDEN_FILE
        + ". A release binary rejects a value its own enum does not declare, costing the same full"
        + " resync as an unknown field (CLOUDP-411951). Accept the new set only once the release a"
        + " rollback lands on is known to declare the added value, or to carry a fallback for it;"
        + " see docs/internal/development/commit-metadata-rollback-policy.md. Derived set:\n"
        + String.join("\n", enumValues);
  }

  /** Reads the golden paths, ignoring the blank and {@code #} comment lines meant for readers. */
  private static ImmutableList<String> readGoldenFile(Path goldenFile) throws IOException {
    return Files.readAllLines(goldenFile, StandardCharsets.UTF_8).stream()
        .map(String::strip)
        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
        .collect(toImmutableList());
  }

  /**
   * Strictly parsed root of the fixture schema, holding a lenient sub-document that in turn holds a
   * strictly parsed one. The walk reads nothing but the {@code Fields} holders, so these three are
   * built like any schema class rather than reduced to declarations: {@code classField} takes a
   * {@link DocumentEncodable}, and a declaration no {@code toBson} or {@code fromBson} reads is an
   * unused field the build rejects.
   */
  private record FixtureRoot(String topLevel, Optional<FixtureLenient> lenient)
      implements DocumentEncodable {

    private static class Fields {
      private static final Field.Required<String> TOP_LEVEL =
          Field.builder("topLevel").stringField().required();

      private static final Field.Optional<FixtureLenient> LENIENT =
          Field.builder("lenient")
              .classField(FixtureLenient::fromBson)
              .allowUnknownFields()
              .optional()
              .noDefault();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.TOP_LEVEL, this.topLevel())
          .field(Fields.LENIENT, this.lenient())
          .build();
    }
  }

  /** Lenient level: its own keys are dropped on rollback, so they go untracked. */
  private record FixtureLenient(String tolerated, Optional<FixtureStrict> strict)
      implements DocumentEncodable {

    private static class Fields {
      private static final Field.Required<String> TOLERATED =
          Field.builder("tolerated").stringField().required();

      private static final Field.Optional<FixtureStrict> STRICT =
          Field.builder("strict")
              .classField(FixtureStrict::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();
    }

    private static FixtureLenient fromBson(DocumentParser parser) throws BsonParseException {
      return new FixtureLenient(
          parser.getField(Fields.TOLERATED).unwrap(), parser.getField(Fields.STRICT).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.TOLERATED, this.tolerated())
          .field(Fields.STRICT, this.strict())
          .build();
    }
  }

  /** Strict level nested under the lenient one, whose keys have to stay tracked. */
  private record FixtureStrict(String rejected) implements DocumentEncodable {

    private static class Fields {
      private static final Field.Required<String> REJECTED =
          Field.builder("rejected").stringField().required();
    }

    private static FixtureStrict fromBson(DocumentParser parser) throws BsonParseException {
      return new FixtureStrict(parser.getField(Fields.REJECTED).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder().field(Fields.REJECTED, this.rejected()).build();
    }
  }
}
