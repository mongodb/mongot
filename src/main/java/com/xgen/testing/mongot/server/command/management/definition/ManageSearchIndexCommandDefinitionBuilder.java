package com.xgen.testing.mongot.server.command.management.definition;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.server.command.management.definition.CreateSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.DropSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition.ListTarget;
import com.xgen.mongot.server.command.management.definition.ManageSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.SearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.UpdateSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.common.NamedSearchIndex;
import com.xgen.mongot.server.command.management.definition.common.UserIndexDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserSearchIndexDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserVectorIndexDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;

public class ManageSearchIndexCommandDefinitionBuilder {
  public enum CommandType {
    CREATE,
    LIST,
    UPDATE,
    DROP
  }

  public static final String DATABASE_NAME = "myDatabase";
  public static final String COLLECTION_NAME = "myCollection";
  public static final String VIEW_NAME = "myView";
  public static final UserViewDefinition VIEW =
      new UserViewDefinition(VIEW_NAME, Optional.of(List.of()));
  public static final UUID COLLECTION_UUID =
      UUID.fromString("beceffcc-66ee-4339-9092-1147075c7602");
  public static final String INDEX_NAME = "myIndex";
  public static final String VECTOR_INDEX_NAME = "myVectorIndex";
  public static final String SEARCH_INDEX_TYPE = "search";
  public static final String VECTOR_INDEX_TYPE = "vectorSearch";
  public static final String VECTOR_FIELD_TYPE = "vector";
  public static final String FILTER_FIELD_TYPE = "filter";
  public static final String VECTOR_PATH = "my.vector";
  public static final int VECTOR_DIMENSIONS = 1000;
  public static final VectorSimilarity VECTOR_SIMILARITY = VectorSimilarity.COSINE;
  public static final VectorQuantization VECTOR_QUANTIZATION = VectorQuantization.NONE;
  private CommandType type;
  private final List<NamedSearchIndex> indexes = new ArrayList<>();
  private Optional<String> name = Optional.empty();
  private Optional<ObjectId> id = Optional.empty();
  private Optional<UserIndexDefinition> definition = Optional.empty();
  private Optional<IndexDefinition.Type> definitionType = Optional.empty();
  private Optional<UserViewDefinition> view = Optional.empty();

  private ManageSearchIndexCommandDefinitionBuilder withType(CommandType type) {
    this.type = type;
    return this;
  }

  public static ManageSearchIndexCommandDefinitionBuilder createIndexes() {
    return new ManageSearchIndexCommandDefinitionBuilder().withType(CommandType.CREATE);
  }

  public static ManageSearchIndexCommandDefinitionBuilder dropIndex() {
    return new ManageSearchIndexCommandDefinitionBuilder().withType(CommandType.DROP);
  }

  public static ManageSearchIndexCommandDefinitionBuilder updateIndex() {
    return new ManageSearchIndexCommandDefinitionBuilder().withType(CommandType.UPDATE);
  }

  public static ManageSearchIndexCommandDefinitionBuilder listAggregation() {
    return new ManageSearchIndexCommandDefinitionBuilder().withType(CommandType.LIST);
  }

  public static ManageSearchIndexCommandDefinitionBuilder listCommand() {
    return new ManageSearchIndexCommandDefinitionBuilder().withType(CommandType.LIST);
  }

  public ManageSearchIndexCommandDefinitionBuilder withDefinition(
      UserIndexDefinition userIndexDefinition) {
    this.definition = Optional.of(userIndexDefinition);
    return this;
  }

  public ManageSearchIndexCommandDefinitionBuilder addIndex(NamedSearchIndex namedSearchIndex) {
    this.indexes.add(namedSearchIndex);
    return this;
  }

  /**
   * Add a new entry to the indexes list with a default name and dynamic mappings enabled.
   *
   * @return this builder
   */
  public ManageSearchIndexCommandDefinitionBuilder withDynamicIndex() {
    this.addIndex(getDynamicIndex());
    return this;
  }

  /**
   * Add a new vector index to the indexes list.
   *
   * @return this builder
   */
  public ManageSearchIndexCommandDefinitionBuilder withVectorIndex() {
    this.addIndex(getVectorIndex());
    return this;
  }

  public static NamedSearchIndex getDynamicIndex() {
    return new NamedSearchIndex(
        INDEX_NAME,
        IndexDefinition.Type.SEARCH,
        new UserSearchIndexDefinition(
            Optional.empty(),
            Optional.empty(),
            DocumentFieldDefinitionBuilder.builder().dynamic(true).build(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            1));
  }

  public static List<VectorIndexFieldDefinition> getVectorIndexFields() {
    return List.of(
        new VectorDataFieldDefinition(
            FieldPath.parse(VECTOR_PATH),
            new VectorFieldSpecification(
                VECTOR_DIMENSIONS,
                VECTOR_SIMILARITY,
                VECTOR_QUANTIZATION,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm())));
  }

  public static NamedSearchIndex getVectorIndex() {
    return new NamedSearchIndex(
        VECTOR_INDEX_NAME,
        IndexDefinition.Type.VECTOR_SEARCH,
        new UserVectorIndexDefinition(
            getVectorIndexFields(), 1, Optional.empty(), Optional.empty()));
  }

  public ManageSearchIndexCommandDefinitionBuilder withIndexName(String indexName) {
    this.name = Optional.of(indexName);
    return this;
  }

  public ManageSearchIndexCommandDefinitionBuilder vectorIndex(String indexName) {
    this.name = Optional.of(indexName);
    this.definitionType = Optional.of(IndexDefinition.Type.VECTOR_SEARCH);
    return this;
  }

  public ManageSearchIndexCommandDefinitionBuilder searchIndex(String indexName) {
    this.name = Optional.of(indexName);
    this.definitionType = Optional.of(IndexDefinition.Type.SEARCH);
    return this;
  }

  public ManageSearchIndexCommandDefinitionBuilder withIndexId(ObjectId indexId) {
    this.id = Optional.of(indexId);
    return this;
  }

  public ManageSearchIndexCommandDefinitionBuilder ofType(IndexDefinition.Type type) {
    this.definitionType = Optional.of(type);
    return this;
  }

  public ManageSearchIndexCommandDefinitionBuilder view(UserViewDefinition view) {
    this.view = Optional.of(view);
    return this;
  }

  public SearchIndexCommandDefinition buildSearchIndexCommand() {
    return switch (this.type) {
      case CREATE -> new CreateSearchIndexesCommandDefinition(COLLECTION_NAME, this.indexes);
      case DROP -> new DropSearchIndexCommandDefinition(COLLECTION_NAME, this.id, this.name);
      case UPDATE ->
          new UpdateSearchIndexCommandDefinition(
              COLLECTION_NAME,
              this.id,
              this.name,
              this.definitionType,
              this.definition.orElseThrow().toBson());
      case LIST -> new ListSearchIndexesCommandDefinition(new ListTarget(this.id, this.name));
    };
  }

  public ManageSearchIndexCommandDefinition build() {
    return new ManageSearchIndexCommandDefinition(
        DATABASE_NAME, COLLECTION_NAME, COLLECTION_UUID, this.view, this.buildSearchIndexCommand());
  }
}
