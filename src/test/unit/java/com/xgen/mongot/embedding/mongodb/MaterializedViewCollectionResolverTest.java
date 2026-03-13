package com.xgen.mongot.embedding.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

/** Unit tests for {@link MaterializedViewCollectionResolver}. */
public class MaterializedViewCollectionResolverTest {

  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private MaterializedViewCollectionMetadataCatalog metadataCatalog;
  private AutoEmbeddingMaterializedViewConfig materializedViewConfig;
  private LeaseManager leaseManager;

  /** Collections "created" by createCollection(); used to build listCollections response. */
  private final List<BsonDocument> collectionInfoDocuments = new ArrayList<>();

  private ListCollectionsIterable<BsonDocument> listCollectionsIterable;
  private static final String MV_DATABASE_NAME = "MaterializedViewCollectionResolverTest";

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    this.mongoClient = mock(MongoClient.class);
    this.mongoDatabase = mock(MongoDatabase.class);
    this.metadataCatalog = new MaterializedViewCollectionMetadataCatalog();
    this.materializedViewConfig = AutoEmbeddingMaterializedViewConfig.getDefault();
    this.leaseManager = mock(LeaseManager.class);

    when(this.mongoClient.getDatabase(eq(MV_DATABASE_NAME))).thenReturn(this.mongoDatabase);

    doAnswer(
            (Answer<Void>)
                inv -> {
                  String name = inv.getArgument(0);
                  UUID uuid = UUID.randomUUID();
                  this.collectionInfoDocuments.add(toCollectionBson(name, uuid));
                  return null;
                })
        .when(this.mongoDatabase)
        .createCollection(anyString());

    this.listCollectionsIterable = mock(ListCollectionsIterable.class);
    when(this.listCollectionsIterable.iterator())
        .thenAnswer(
            inv -> {
              List<BsonDocument> snapshot = new ArrayList<>(this.collectionInfoDocuments);
              MongoCursor<BsonDocument> cursor = mock(MongoCursor.class);
              AtomicInteger index = new AtomicInteger(0);
              when(cursor.hasNext()).thenAnswer(inv2 -> index.get() < snapshot.size());
              when(cursor.next()).thenAnswer(inv2 -> snapshot.get(index.getAndIncrement()));
              return cursor;
            });
    when(this.listCollectionsIterable.filter(any())).thenReturn(this.listCollectionsIterable);
    when(this.listCollectionsIterable.into(anyCollection()))
        .thenAnswer(
            inv -> {
              Collection<BsonDocument> target = inv.getArgument(0);
              target.clear();
              target.addAll(new ArrayList<>(this.collectionInfoDocuments));
              return target;
            });
    when(this.mongoDatabase.listCollections(BsonDocument.class))
        .thenReturn(this.listCollectionsIterable);
    when(this.leaseManager.initializeLease(
            any(IndexDefinitionGeneration.class), any(MaterializedViewCollectionMetadata.class)))
        .thenAnswer(inv -> inv.getArgument(1));
  }

  private static BsonDocument toCollectionBson(String name, UUID uuid) {
    return new BsonDocument()
        .append("type", new BsonString("collection"))
        .append("name", new BsonString(name))
        .append("info", new MongoDbCollectionInfo.Collection.Info(uuid).toBson());
  }

  private MaterializedViewIndexDefinitionGeneration createIndexDefinitionGeneration(
      VectorIndexDefinition definition) {
    return new MaterializedViewIndexDefinitionGeneration(
        definition, new MaterializedViewGeneration(Generation.CURRENT));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void
      getOrCreateMaterializedViewForIndex_noExistingCollections_createsNewAndReturnsMetadata() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);
    // collectionInfoDocuments is empty -> listCollections().filter().into() yields no names

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            MV_DATABASE_NAME,
            this.mongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    assertThat(metadata.collectionName()).startsWith(indexId.toHexString());
    assertThat(metadata.collectionName()).contains("-");
    assertThat(metadata.collectionUuid()).isNotNull();

    ArgumentCaptor<String> createCollectionCaptor = ArgumentCaptor.forClass(String.class);
    verify(this.mongoDatabase).createCollection(createCollectionCaptor.capture());
    assertThat(createCollectionCaptor.getValue()).isEqualTo(metadata.collectionName());

    assertThat(this.metadataCatalog.getMetadata(indexDefGen.getGenerationId())).isEqualTo(metadata);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_legacySingleCollectionName_reusesCollection() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    String legacyCollectionName = indexId.toHexString();
    UUID existingUuid = UUID.randomUUID();
    this.collectionInfoDocuments.add(toCollectionBson(legacyCollectionName, existingUuid));

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            MV_DATABASE_NAME,
            this.mongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    assertThat(metadata.collectionName()).isEqualTo(legacyCollectionName);
    assertThat(metadata.collectionUuid()).isEqualTo(existingUuid);
    verify(this.mongoDatabase, org.mockito.Mockito.never()).createCollection(anyString());
    assertThat(this.metadataCatalog.getMetadata(indexDefGen.getGenerationId())).isEqualTo(metadata);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_existingMatchingHash_reusesCollection() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    // First call: collectionInfoDocuments empty -> resolver creates collection (adds to
    // collectionInfoDocuments).
    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            MV_DATABASE_NAME,
            this.mongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata first =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);
    String createdCollectionName = first.collectionName();
    UUID createdUuid = first.collectionUuid();

    // Second call: listCollections yields only the created collection so resolver reuses it.
    this.collectionInfoDocuments.clear();
    this.collectionInfoDocuments.add(toCollectionBson(createdCollectionName, createdUuid));

    MaterializedViewCollectionMetadata second =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    assertThat(second.collectionName()).isEqualTo(createdCollectionName);
    assertThat(second.collectionUuid()).isEqualTo(createdUuid);
    verify(this.mongoDatabase).createCollection(anyString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_sameDefinitionTwice_returnsConsistentName() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);
    // collectionInfoDocuments empty -> both calls see no existing collections, create same name

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            MV_DATABASE_NAME,
            this.mongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata1 =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);
    MaterializedViewCollectionMetadata metadata2 =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    assertThat(metadata1.collectionName()).isEqualTo(metadata2.collectionName());
    assertThat(this.metadataCatalog.getMetadata(indexDefGen.getGenerationId())).isNotNull();
    assertThat(this.metadataCatalog.getMetadata(indexDefGen.getGenerationId()).collectionName())
        .isEqualTo(metadata1.collectionName());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_filterFieldChange_reusesExistingCollection() {
    // Hash only includes auto-embed fields (path, model, modality), not filter path.
    // So changing filter path alone must reuse the same MV collection.
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definitionWithFilterA =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withFilterPath("filter.a")
            .withAutoEmbedField("embeddingField")
            .build();
    VectorIndexDefinition definitionWithFilterB =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withFilterPath("filter.b")
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGenA = createIndexDefinitionGeneration(definitionWithFilterA);
    var indexDefGenB = createIndexDefinitionGeneration(definitionWithFilterB);

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            MV_DATABASE_NAME,
            this.mongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadataA =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGenA);
    String collectionNameA = metadataA.collectionName();
    UUID uuidA = metadataA.collectionUuid();

    // Pre-seat existing collection so resolver takes "reuse by hash" path for second definition.
    this.collectionInfoDocuments.clear();
    this.collectionInfoDocuments.add(toCollectionBson(collectionNameA, uuidA));

    MaterializedViewCollectionMetadata metadataB =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGenB);

    assertThat(metadataB.collectionName()).isEqualTo(collectionNameA);
    assertThat(metadataB.collectionUuid()).isEqualTo(uuidA);
    // createCollection called only once (for the first definition).
    verify(this.mongoDatabase).createCollection(anyString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_autoEmbedFieldChange_createsNewCollection() {
    // Hash includes auto-embed field path (and model, modality). Changing auto-embed field
    // produces a different hash, so a new MV collection must be created.
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definitionEmbedA =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingFieldA")
            .build();
    VectorIndexDefinition definitionEmbedB =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingFieldB")
            .build();
    var indexDefGenA = createIndexDefinitionGeneration(definitionEmbedA);
    var indexDefGenB = createIndexDefinitionGeneration(definitionEmbedB);

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            MV_DATABASE_NAME,
            this.mongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadataA =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGenA);
    String collectionNameA = metadataA.collectionName();

    // Pre-seat the first collection so resolver sees existing collections.
    this.collectionInfoDocuments.clear();
    this.collectionInfoDocuments.add(toCollectionBson(collectionNameA, metadataA.collectionUuid()));

    MaterializedViewCollectionMetadata metadataB =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGenB);

    assertThat(metadataB.collectionName()).isNotEqualTo(collectionNameA);
    assertThat(metadataB.collectionName()).startsWith(indexId.toHexString());
    // createCollection called twice: once for A, once for B (new hash).
    verify(this.mongoDatabase, org.mockito.Mockito.times(2)).createCollection(anyString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_getCollectionInfoFails_throwsException() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    // Legacy path: first listCollections() (with filter) must return one doc so resolver takes
    // legacy path; second listCollections() (no filter, from getCollectionInfo) must return empty
    // so getCollectionInfo throws.
    String legacyCollectionName = indexId.toHexString();
    BsonDocument legacyDoc = toCollectionBson(legacyCollectionName, UUID.randomUUID());
    ListCollectionsIterable<BsonDocument> oneDocIterable = mock(ListCollectionsIterable.class);
    MongoCursor<BsonDocument> oneDocCursor = mock(MongoCursor.class);
    when(oneDocCursor.hasNext()).thenReturn(true).thenReturn(false);
    when(oneDocCursor.next()).thenReturn(legacyDoc);
    when(oneDocIterable.iterator()).thenReturn(oneDocCursor);
    when(oneDocIterable.filter(any())).thenReturn(oneDocIterable);
    when(oneDocIterable.into(anyCollection()))
        .thenAnswer(
            inv -> {
              Collection<BsonDocument> target = inv.getArgument(0);
              target.clear();
              target.add(legacyDoc);
              return target;
            });
    this.collectionInfoDocuments.clear();
    when(this.mongoDatabase.listCollections(BsonDocument.class))
        .thenReturn(oneDocIterable)
        .thenReturn(this.listCollectionsIterable);

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            MV_DATABASE_NAME,
            this.mongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewNonTransientException thrown =
        assertThrows(
            MaterializedViewNonTransientException.class,
            () -> resolver.getOrCreateMaterializedViewForIndex(indexDefGen));

    assertThat(thrown.getCause()).isNotNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void
      getOrCreateMaterializedViewForIndex_schemaVersion0_returnsVersionZeroMetadata() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    AutoEmbeddingMaterializedViewConfig configV0 =
        AutoEmbeddingMaterializedViewConfig.create(
            CommonReplicationConfig.defaultGlobalReplicationConfig(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(0),
            Optional.empty());

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            MV_DATABASE_NAME, this.mongoClient, this.metadataCatalog, configV0, this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    assertThat(metadata.schemaMetadata().materializedViewSchemaVersion()).isEqualTo(0L);
    assertThat(metadata.schemaMetadata().autoEmbeddingFieldsMapping()).isEmpty();
    assertThat(metadata.schemaMetadata())
        .isEqualTo(
            MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void
      getOrCreateMaterializedViewForIndex_schemaVersion1_returnsAutoEmbedFieldMapping() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .withFilterPath("filterField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    AutoEmbeddingMaterializedViewConfig configV1 =
        AutoEmbeddingMaterializedViewConfig.create(
            CommonReplicationConfig.defaultGlobalReplicationConfig(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(1),
            Optional.empty());

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            MV_DATABASE_NAME, this.mongoClient, this.metadataCatalog, configV1, this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    assertThat(metadata.schemaMetadata().materializedViewSchemaVersion()).isEqualTo(1L);
    assertThat(metadata.schemaMetadata().autoEmbeddingFieldsMapping()).isNotEmpty();
    assertThat(metadata.schemaMetadata().autoEmbeddingFieldsMapping())
        .containsKey(FieldPath.parse("embeddingField"));
    assertThat(metadata.schemaMetadata().autoEmbeddingFieldsMapping())
        .containsEntry(
            FieldPath.parse("embeddingField"), FieldPath.parse("_autoEmbed.embeddingField"));
    assertThat(metadata.schemaMetadata().autoEmbeddingFieldsMapping().size()).isEqualTo(1);
    // Filter fields should NOT be in the mapping
    assertThat(metadata.schemaMetadata().autoEmbeddingFieldsMapping())
        .doesNotContainKey(FieldPath.parse("filterField"));
  }
}
