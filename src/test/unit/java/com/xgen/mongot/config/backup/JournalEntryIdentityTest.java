package com.xgen.mongot.config.backup;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

public class JournalEntryIdentityTest {

  @Test
  public void thinParseSearchEntry_readsIdentity() throws Exception {
    BsonDocument entry = SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.toBson();

    JournalEntryIdentity identity = thinParseSearch(entry);

    assertThat(identity)
        .isEqualTo(JournalEntryIdentity.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION));
  }

  @Test
  public void thinParseVectorEntry_readsIdentity() throws Exception {
    BsonDocument entry = VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.toBson();

    JournalEntryIdentity identity =
        JournalEntryIdentity.thinParseVectorEntry(BsonParseContext.root(), entry);

    assertThat(identity)
        .isEqualTo(JournalEntryIdentity.of(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION));
  }

  @Test
  public void thinParseSearchEntry_unparseableMappings_readsIdentity() throws Exception {
    BsonDocument entry = SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.toBson();
    entry.getDocument("index").append("mappings", new BsonString("not a mapping"));

    JournalEntryIdentity identity = thinParseSearch(entry);

    assertThat(identity.indexId()).isEqualTo(SearchIndex.MOCK_INDEX_ID);
  }

  @Test
  public void thinParseSearchEntry_unknownTopLevelField_readsIdentity() throws Exception {
    BsonDocument entry =
        SearchIndex.MOCK_INDEX_DEFINITION_GENERATION
            .toBson()
            .append("fieldFromTheFuture", new BsonBoolean(true));

    JournalEntryIdentity identity = thinParseSearch(entry);

    assertThat(identity.indexId()).isEqualTo(SearchIndex.MOCK_INDEX_ID);
  }

  @Test
  public void thinParseSearchEntry_unknownFormatVersion_readsIdentity() throws Exception {
    BsonDocument entry = SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.toBson();
    entry.getDocument("generation").append("formatVersion", new BsonInt32(99));

    JournalEntryIdentity identity = thinParseSearch(entry);

    assertThat(identity.generation().indexFormatVersion.versionNumber).isEqualTo(99);
  }

  @Test
  public void thinParseSearchEntry_missingIndexId_throws() {
    BsonDocument entry = SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.toBson();
    entry.getDocument("index").remove("indexID");

    TestUtils.assertThrows(
        "\"index.indexID\" is required", BsonParseException.class, () -> thinParseSearch(entry));
  }

  @Test
  public void thinParseSearchEntry_missingGeneration_throws() {
    BsonDocument entry = SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.toBson();
    entry.remove("generation");

    TestUtils.assertThrows("is required", BsonParseException.class, () -> thinParseSearch(entry));
  }

  @Test
  public void thinParseSearchEntry_unparseableGeneration_throws() {
    BsonDocument entry =
        SearchIndex.MOCK_INDEX_DEFINITION_GENERATION
            .toBson()
            .append("generation", new BsonString("not a generation"));

    TestUtils.assertThrows(
        "must be a document", BsonParseException.class, () -> thinParseSearch(entry));
  }

  @Test
  public void namespaceKey_sameNamespaceAndName_matchesParsedTwin() {
    JournalEntryIdentity identity =
        JournalEntryIdentity.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION);

    assertThat(identity.namespaceKey())
        .isEqualTo(
            String.format(
                "(%s %s %s)", identity.database(), identity.collectionUuid(), identity.name()));
  }

  @Test
  public void generationId_matchesDefinitionGeneration() {
    Generation generation = SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.generation();

    JournalEntryIdentity identity =
        JournalEntryIdentity.of(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION);

    assertThat(identity.generationId())
        .isEqualTo(generation.generationId(SearchIndex.MOCK_INDEX_ID));
  }

  private static JournalEntryIdentity thinParseSearch(BsonDocument entry)
      throws BsonParseException {
    return JournalEntryIdentity.thinParseSearchEntry(BsonParseContext.root(), entry);
  }
}
