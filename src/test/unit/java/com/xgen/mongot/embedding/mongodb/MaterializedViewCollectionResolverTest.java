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
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.common.DefaultInternalDatabaseResolver;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

  private AutoEmbeddingMongoClient autoEmbeddingMongoClient;
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

    this.mongoDatabase = mock(MongoDatabase.class);
    this.metadataCatalog = new MaterializedViewCollectionMetadataCatalog();
    this.materializedViewConfig = AutoEmbeddingMaterializedViewConfig.getDefault();
    this.leaseManager = mock(LeaseManager.class);
    var mongoClient = mock(MongoClient.class);
    this.autoEmbeddingMongoClient =
        new AutoEmbeddingMongoClient(
            mock(SyncSourceConfig.class),
            mongoClient,
            mongoClient,
            mongoClient,
            new SimpleMeterRegistry());
    when(mongoClient.getDatabase(eq(MV_DATABASE_NAME))).thenReturn(this.mongoDatabase);

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
            any(MaterializedViewIndexDefinitionGeneration.class),
            any(MaterializedViewCollectionMetadata.class)))
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
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
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
            .materializedViewNameFormatVersion(0L)
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    String legacyCollectionName = indexId.toHexString();
    UUID existingUuid = UUID.randomUUID();
    this.collectionInfoDocuments.add(toCollectionBson(legacyCollectionName, existingUuid));

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
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
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
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
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
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
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
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
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
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

    // First listCollections() call is from getCollectionInfos in getOrCreateCollectionName:
    // returns empty so the collection is not found and createCollection is called.
    // Second listCollections() call is from getCollectionInfo after the name is resolved:
    // returns an empty iterable so orElseThrow() throws NoSuchElementException,
    // which gets wrapped in MaterializedViewTransientException.
    ListCollectionsIterable<BsonDocument> emptyIterable = mock(ListCollectionsIterable.class);
    MongoCursor<BsonDocument> emptyCursor = mock(MongoCursor.class);
    when(emptyCursor.hasNext()).thenReturn(false);
    when(emptyIterable.iterator()).thenReturn(emptyCursor);
    when(emptyIterable.filter(any())).thenReturn(emptyIterable);

    this.collectionInfoDocuments.clear();
    when(this.mongoDatabase.listCollections(BsonDocument.class))
        .thenReturn(this.listCollectionsIterable) // getOrCreateCollectionName: empty → creates
        .thenReturn(emptyIterable); // getCollectionInfo: empty → throws

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewTransientException thrown =
        assertThrows(
            MaterializedViewTransientException.class,
            () -> resolver.getOrCreateMaterializedViewForIndex(indexDefGen));

    assertThat(thrown.getCause()).isNotNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_schemaVersion0_returnsVersionZeroMetadata() {
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
            Optional.empty(),
            Optional.empty(),
            Optional.of(0),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            configV0,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    assertThat(metadata.schemaMetadata().materializedViewSchemaVersion()).isEqualTo(0L);
    assertThat(metadata.schemaMetadata().autoEmbeddingFieldsMapping()).isEmpty();
    assertThat(metadata.schemaMetadata())
        .isEqualTo(MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_schemaVersion1_returnsAutoEmbedFieldMapping() {
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
            Optional.empty(),
            Optional.empty(),
            Optional.of(1),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            configV1,
            this.leaseManager);

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

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_defaultNameFormatVersion0_usesIndexIdOnly() {
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
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(0L),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            configV0,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    // When defaultMaterializedViewNameFormatVersion is 0, collection name should be just the
    // indexId hex string without any hash suffix.
    assertThat(metadata.collectionName()).isEqualTo(indexId.toHexString());
    assertThat(metadata.collectionName()).doesNotContain("-");
    assertThat(metadata.collectionUuid()).isNotNull();

    ArgumentCaptor<String> createCollectionCaptor = ArgumentCaptor.forClass(String.class);
    verify(this.mongoDatabase).createCollection(createCollectionCaptor.capture());
    assertThat(createCollectionCaptor.getValue()).isEqualTo(indexId.toHexString());

    assertThat(this.metadataCatalog.getMetadata(indexDefGen.getGenerationId())).isEqualTo(metadata);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_defaultNameFormatVersion1_usesHashFormat() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    // Explicitly set defaultMaterializedViewNameFormatVersion to 1
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
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(1L),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            configV1,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    // When defaultMaterializedViewNameFormatVersion is 1, collection name should include
    // indexId-hash-matViewNameFormatVersion-autoEmbeddingDefinitionVersion format.
    assertThat(metadata.collectionName()).startsWith(indexId.toHexString());
    assertThat(metadata.collectionName()).contains("-");
    // autoEmbeddingDefinitionVersion defaults to 0 when not set on index definition.
    assertThat(metadata.collectionName()).endsWith("-1-0");
    assertThat(metadata.collectionUuid()).isNotNull();

    assertThat(this.metadataCatalog.getMetadata(indexDefGen.getGenerationId())).isEqualTo(metadata);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_defaultConfig_usesHashFormat() {
    // Default config (getDefault()) should use version 1 (hash format)
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    // Use default config which should have defaultMaterializedViewNameFormatVersion = 1
    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    // Default config should produce hash-format collection names (version 1)
    assertThat(metadata.collectionName()).startsWith(indexId.toHexString());
    assertThat(metadata.collectionName()).contains("-");
    assertThat(metadata.collectionUuid()).isNotNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void
      getOrCreateMaterializedViewForIndex_perIndexNameFormatVersionOverridesConfigDefault() {
    // Per-index materializedViewNameFormatVersion=0 should override config default of 1,
    // producing a collection name that is just the indexId (no hash suffix).
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .materializedViewNameFormatVersion(0L)
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    // Config default is 1 (hash format), but per-index override is 0.
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
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(1L),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            configV1,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    // Per-index v0 means collection name is just the indexId hex string.
    assertThat(metadata.collectionName()).isEqualTo(indexId.toHexString());
    assertThat(metadata.collectionName()).doesNotContain("-");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void
      getOrCreateMaterializedViewForIndex_perIndexNameFormatVersion1_overridesConfigDefault0() {
    // Per-index materializedViewNameFormatVersion=1 should override config default of 0,
    // producing a hash-format collection name.
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .materializedViewNameFormatVersion(1L)
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    // Config default is 0 (legacy), but per-index override is 1.
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
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(0L),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            configV0,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    // Per-index v1 means hash-format collection name.
    assertThat(metadata.collectionName()).startsWith(indexId.toHexString());
    assertThat(metadata.collectionName()).contains("-");
    assertThat(metadata.collectionName()).endsWith("-1-0");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void
      getOrCreateMaterializedViewForIndex_autoEmbeddingDefinitionVersionAppearsInCollectionName() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .autoEmbeddingDefinitionVersion(3L)
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    // Collection name format: indexId-hash-matViewNameFormatVersion-autoEmbeddingDefinitionVersion
    assertThat(metadata.collectionName()).startsWith(indexId.toHexString());
    assertThat(metadata.collectionName()).endsWith("-1-3");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_differentAutoEmbeddingDefinitionVersion() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definitionV1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .autoEmbeddingDefinitionVersion(1L)
            .build();
    VectorIndexDefinition definitionV2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .autoEmbeddingDefinitionVersion(2L)
            .build();
    var indexDefGenV1 = createIndexDefinitionGeneration(definitionV1);
    var indexDefGenV2 = createIndexDefinitionGeneration(definitionV2);

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadataV1 =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGenV1);
    MaterializedViewCollectionMetadata metadataV2 =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGenV2);

    // Same index, same fields, but different autoEmbeddingDefinitionVersion -> different names.
    assertThat(metadataV1.collectionName()).isNotEqualTo(metadataV2.collectionName());
    assertThat(metadataV1.collectionName()).endsWith("-1-1");
    assertThat(metadataV2.collectionName()).endsWith("-1-2");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void
      getOrCreateMaterializedViewForIndex_noAutoEmbeddingDefinitionVersion_defaultsToZero() {
    // When autoEmbeddingDefinitionVersion is not set, it defaults to 0.
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition definitionNoVersion =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .build();
    VectorIndexDefinition definitionExplicitZero =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withAutoEmbedField("embeddingField")
            .autoEmbeddingDefinitionVersion(0L)
            .build();
    var indexDefGenNoVersion = createIndexDefinitionGeneration(definitionNoVersion);
    var indexDefGenExplicitZero = createIndexDefinitionGeneration(definitionExplicitZero);

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadataNoVersion =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGenNoVersion);
    MaterializedViewCollectionMetadata metadataExplicitZero =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGenExplicitZero);

    // Both should produce the same collection name since default is 0.
    assertThat(metadataNoVersion.collectionName()).isEqualTo(metadataExplicitZero.collectionName());
    assertThat(metadataNoVersion.collectionName()).endsWith("-1-0");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_indexIdAtCreationTimeOverridesIndexId_v0() {
    // When indexIDAtCreationTime is set, it should be used as the collection name prefix
    // instead of the current indexId, for both v0 and v1 name formats.
    ObjectId indexId = new ObjectId();
    ObjectId indexIdAtCreation = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .indexIdAtCreationTime(indexIdAtCreation)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    // v0: collection name should be just the indexIdAtCreationTime hex string.
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
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(0L),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            configV0,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    // v0: collection name is the indexIdAtCreationTime, not the current indexId.
    assertThat(metadata.collectionName()).isEqualTo(indexIdAtCreation.toHexString());
    assertThat(metadata.collectionName()).doesNotContain(indexId.toHexString());
    assertThat(metadata.collectionName()).doesNotContain("-");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getOrCreateMaterializedViewForIndex_indexIdAtCreationTimeOverridesIndexId_v1() {
    ObjectId indexId = new ObjectId();
    ObjectId indexIdAtCreation = new ObjectId();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .indexIdAtCreationTime(indexIdAtCreation)
            .withAutoEmbedField("embeddingField")
            .build();
    var indexDefGen = createIndexDefinitionGeneration(definition);

    // v1 (default config): collection name should start with indexIdAtCreationTime.
    MaterializedViewCollectionResolver resolver =
        new MaterializedViewCollectionResolver(
            new DefaultInternalDatabaseResolver(MV_DATABASE_NAME),
            this.autoEmbeddingMongoClient,
            this.metadataCatalog,
            this.materializedViewConfig,
            this.leaseManager);

    MaterializedViewCollectionMetadata metadata =
        resolver.getOrCreateMaterializedViewForIndex(indexDefGen);

    // v1: collection name starts with indexIdAtCreationTime, not the current indexId.
    assertThat(metadata.collectionName()).startsWith(indexIdAtCreation.toHexString());
    assertThat(metadata.collectionName().startsWith(indexId.toHexString())).isFalse();
    assertThat(metadata.collectionName()).contains("-");
    assertThat(metadata.collectionName()).endsWith("-1-0");
  }

  @Test
  public void computeHash_legacyTextFieldSpecification_throwsIllegalArgumentException() {
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder().withTextField("legacyTextField").build();

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MaterializedViewCollectionResolver.computeHash(
                    AutoEmbedFieldMappingCreator.createAutoEmbedMapping(definition)));

    assertThat(thrown.getMessage()).contains("Legacy TEXT field specification");
  }
}
