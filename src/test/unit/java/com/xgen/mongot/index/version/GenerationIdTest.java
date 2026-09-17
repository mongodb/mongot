package com.xgen.mongot.index.version;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Sets;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.junit.Test;

public class GenerationIdTest {
  @Test
  public void testConstructor() {
    ObjectId objectId = new ObjectId();
    GenerationId value = GenerationIdBuilder.create(objectId, 1, 2, 1);
    assertEquals(objectId, value.indexId);
    assertEquals(1, value.generation.userIndexVersion.versionNumber);
    assertEquals(2, value.generation.formatVersion.versionNumber());
    assertEquals(1, value.generation.attemptNumber);
  }

  public static List<CheckedSupplier<GenerationId, RuntimeException>>
      distinctGenerationIdSuppliers() {
    Set<ObjectId> objectIds = Set.of(new ObjectId(), new ObjectId());
    Set<Integer> userVersions = Set.of(1, 10);
    Set<Integer> indexFormatVersions = Set.of(2, 10);
    Set<Integer> attempts = Set.of(1, 2);

    // Take the Cartesian product of all the userVersion, indexFormatVersions, and attempts.
    List<Generation> distinctGenerations =
        Sets.cartesianProduct(userVersions, indexFormatVersions, attempts).stream()
            .map(
                params ->
                    new Generation(
                        new UserIndexVersion(params.get(0)),
                        IndexFormatVersion.create(params.get(1)),
                        params.get(2)))
            .collect(Collectors.toList());

    // Now we Cartesian product the Generations with the ObjectIds to get GenerationId suppliers.
    return objectIds.stream()
        .flatMap(
            objectId ->
                distinctGenerations.stream()
                    .<CheckedSupplier<GenerationId, RuntimeException>>map(
                        generation -> () -> new GenerationId(objectId, generation)))
        .collect(Collectors.toList());
  }

  @Test
  public void testEquality() {
    TestUtils.assertEqualityGroups(distinctGenerationIdSuppliers());
  }

  /** The f segment is the engine's own format version. */
  @Test
  public void uniqueString_usesTheEnginesFormatVersion() {
    ObjectId indexId = new ObjectId();
    var lucene =
        new GenerationId(
            indexId, new Generation(new UserIndexVersion(1), IndexFormatVersion.SIX, 2));
    assertEquals(indexId.toHexString() + "-f6-u1-a2", lucene.uniqueString());

    var vectorlite =
        new GenerationId(
            indexId, new Generation(new UserIndexVersion(1), new VectorliteFormatVersion(3)));
    assertEquals(indexId.toHexString() + "-f3-u1-a0", vectorlite.uniqueString());
  }

  /** Ensure that distinct GenerationIds have distinct uniqueStrings. */
  @Test
  public void testUniqueStringDistinctness() {
    List<String> uniqueStrings =
        distinctGenerationIdSuppliers().stream()
            .map(CheckedSupplier::get)
            .map(GenerationId::uniqueString)
            .collect(Collectors.toList());
    Set<String> uniqueStringSet = new HashSet<>(uniqueStrings);

    assertEquals("Log strings should all be unique.", uniqueStrings.size(), uniqueStringSet.size());
  }
}
