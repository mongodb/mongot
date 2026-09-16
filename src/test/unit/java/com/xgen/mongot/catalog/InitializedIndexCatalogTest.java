package com.xgen.mongot.catalog;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import org.bson.types.ObjectId;
import org.junit.Test;

public class InitializedIndexCatalogTest {
  @Test
  public void testGetIndex() {
    InitializedIndexCatalog catalog = new InitializedIndexCatalog();
    VectorIndexDefinitionGeneration generation =
        new VectorIndexDefinitionGeneration(VectorIndex.MOCK_VECTOR_DEFINITION, Generation.FIRST);
    IndexGeneration index = new IndexGeneration(VectorIndex.mockIndex(generation), generation);
    InitializedIndex initializedIndex =
        VectorIndex.mockInitializedIndex(index.getIndex().asVectorIndex(), index.getGenerationId());
    catalog.addIndex(initializedIndex);

    Optional<InitializedIndex> foundIndex = catalog.getIndex(generation.getGenerationId());
    assertTrue(foundIndex.isPresent());
    assertEquals(initializedIndex, foundIndex.get());
  }

  @Test
  public void testRemoveIndex() {
    InitializedIndexCatalog catalog = new InitializedIndexCatalog();
    VectorIndexDefinitionGeneration generation =
        new VectorIndexDefinitionGeneration(VectorIndex.MOCK_VECTOR_DEFINITION, Generation.FIRST);
    IndexGeneration index = new IndexGeneration(VectorIndex.mockIndex(generation), generation);
    InitializedVectorIndex initializedVectorIndex =
        VectorIndex.mockInitializedIndex(index.getIndex().asVectorIndex(), index.getGenerationId());

    catalog.addIndex(initializedVectorIndex);
    assertTrue(catalog.getIndex(generation.getGenerationId()).isPresent());

    assertTrue(catalog.removeIndex(generation.getGenerationId()).isPresent());
    assertFalse(catalog.getIndex(generation.getGenerationId()).isPresent());
    assertFalse(catalog.removeIndex(generation.getGenerationId()).isPresent());
  }

  @Test
  public void testAddMultipleIndexesSameId() {
    ObjectId indexId = new ObjectId();
    InitializedIndexCatalog catalog = new InitializedIndexCatalog();
    IndexGeneration generation = mockIndexGeneration(indexId);

    catalog.addIndex(mockInitializedIndex(generation));
    assertTrue(catalog.getIndex(generation.getGenerationId()).isPresent());
    IndexGeneration newGeneration = mockIndexGeneration(indexId, 2);
    InitializedSearchIndex initializedIndex = mockInitializedIndex(newGeneration);
    assertEquals(newGeneration.getGenerationId(), initializedIndex.getGenerationId());
    catalog.addIndex(initializedIndex);
    assertTrue(catalog.getIndex(newGeneration.getGenerationId()).isPresent());

    catalog.removeIndex(generation.getGenerationId());
    assertFalse(catalog.getIndex(generation.getGenerationId()).isPresent());
    assertTrue(catalog.getIndex(newGeneration.getGenerationId()).isPresent());
  }

  @Test
  public void testGetIndexesReturnsEveryGeneration() {
    ObjectId indexId = new ObjectId();
    InitializedIndexCatalog catalog = new InitializedIndexCatalog();
    InitializedSearchIndex firstGeneration = mockInitializedIndex(mockIndexGeneration(indexId));
    InitializedSearchIndex secondGeneration = mockInitializedIndex(mockIndexGeneration(indexId, 2));
    InitializedSearchIndex otherIndex = mockInitializedIndex(mockIndexGeneration());
    catalog.addIndex(firstGeneration);
    catalog.addIndex(secondGeneration);
    catalog.addIndex(otherIndex);

    assertEquals(
        Set.of(firstGeneration, secondGeneration, otherIndex), Set.copyOf(catalog.getIndexes()));
  }

  @Test
  public void testGetIndexesIsSnapshot() {
    InitializedIndexCatalog catalog = new InitializedIndexCatalog();
    catalog.addIndex(mockInitializedIndex(mockIndexGeneration()));

    Collection<InitializedIndex> indexes = catalog.getIndexes();
    indexes.clear();

    assertEquals(1, catalog.getIndexes().size());
  }

  @Test
  public void testAddSameGenerationIdThrows() {
    InitializedIndexCatalog catalog = new InitializedIndexCatalog();
    IndexGeneration generation = mockIndexGeneration();
    catalog.addIndex(mockInitializedIndex(generation));
    TestUtils.assertThrows(
        "already contains index of id: " + generation.getGenerationId(),
        IllegalStateException.class,
        () -> {
          catalog.addIndex(mockInitializedIndex(generation));
        });
  }
}
