package com.xgen.mongot.config.manager;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGenerationProducer;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/**
 * ConfigManagerChangePlan describes the difference between the current state of a ConfigManager and
 * the desired state supplied to it via ConfigManager::update.
 */
record ConfigManagerChangePlan(
    List<IndexDefinitionGenerationProducer> addedIndexDefinitions,
    List<ObjectId> droppedIndexes,
    List<ModifiedIndexInformation> modifiedIndexes,
    List<ObjectId> unmodifiedIndexes)
    implements DocumentEncodable {

  private static class Fields {
    private static final Field.Required<List<IndexDefinition>> ADDED_INDEX_DEFINITIONS =
        Field.builder("addedIndexDefinitions")
            .classField(
                d -> Check.unreachable("Deserialization not supported for IndexDefinition"),
                IndexDefinition::toBson)
            .disallowUnknownFields()
            .asList()
            .required();

    private static final Field.Required<List<ObjectId>> DROPPED_INDEXES =
        Field.builder("droppedIndexes").objectIdField().asList().required();

    private static final Field.Required<List<ModifiedIndexInformation>> MODIFIED_INDEXES =
        Field.builder("modifiedIndexes")
            /*
             * We only need the encoder for ModifiedIndexInformation, so use classField() to get
             * at one. This should be cleaned up when converting parsers/encoders to Codecs.
             */
            .classField(
                d ->
                    Check.unreachable("Deserialization not supported for ModifiedIndexInformation"),
                ModifiedIndexInformation::toBson)
            .disallowUnknownFields()
            .asList()
            .required();

    private static final Field.Required<List<ObjectId>> UNMODIFIED_INDEXES =
        Field.builder("unmodifiedIndexes").objectIdField().asList().required();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(
            Fields.ADDED_INDEX_DEFINITIONS,
            this.addedIndexDefinitions.stream()
                .map(IndexDefinitionGenerationProducer::getIndexDefinition)
                .collect(Collectors.toList()))
        .field(Fields.DROPPED_INDEXES, this.droppedIndexes)
        .field(Fields.MODIFIED_INDEXES, this.modifiedIndexes)
        .field(Fields.UNMODIFIED_INDEXES, this.unmodifiedIndexes)
        .build();
  }

  public boolean hasChanges() {
    return !(this.droppedIndexes.isEmpty()
        && this.modifiedIndexes.isEmpty()
        && this.addedIndexDefinitions.isEmpty());
  }
}
