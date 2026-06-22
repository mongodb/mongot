package com.xgen.mongot.config.manager;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGenerationProducer;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/**
 * Contains both the existing and desired definitions for indexes whose definitions were modified in
 * a way that requires re-indexing or hot swaps, as well as the differences between said
 * definitions.
 *
 * <p>Different variants of this class represent combinations of equivalence relationships between
 * the desired definition and both the live and staged existing indexes.
 */
abstract class ModifiedIndexInformation implements DocumentEncodable {

  private static class Fields {
    private static final Field.Required<ObjectId> INDEX_ID =
        Field.builder("indexId").objectIdField().required();

    private static final Field.Required<List<IndexDefinition>> EXISTING_DEFINITIONS =
        Field.builder("existingDefinitions")
            .classField(
                ignored -> Check.unreachable("Deserialization not supported for IndexDefinition"),
                IndexDefinition::toBson)
            .disallowUnknownFields()
            .asList()
            .required();

    private static final Field.Required<IndexDefinitionGenerationProducer> DESIRED_DEFINITION =
        Field.builder("desiredDefinition")
            .classField(
                ignored ->
                    Check.unreachable(
                        "Deserialization not supported for IndexDefinitionGenerationProducer"),
                (IndexDefinitionGenerationProducer producer) ->
                    producer.getIndexDefinition().toBson())
            .disallowUnknownFields()
            .required();

    private static final Field.Required<List<String>> REASONS =
        Field.builder("reasons").stringField().asList().required();

    private static final Field.Required<Type> TYPE =
        Field.builder("type").enumField(Type.class).asUpperUnderscore().required();
  }

  public enum Type {
    /** There is no staged swap, the new definition differs from the existing live one. */
    DIFFERENT_FROM_LIVE_NO_STAGED,
    /**
     * For a previously staged swap, the desired index definition is the same as the index in the
     * catalog.
     */
    SAME_AS_LIVE_DIFFERENT_FROM_STAGED,
    /**
     * For a previously staged swap, the desired index definition differs from both the staged and
     * the live index.
     */
    DIFFERENT_FROM_BOTH
  }

  private final IndexGeneration liveIndex;
  private final IndexDefinitionGenerationProducer desiredDefinition;
  private final List<String> reasons;

  ModifiedIndexInformation(
      IndexDefinitionGenerationProducer desiredDefinition,
      List<String> reasons,
      IndexGeneration liveIndex) {
    Check.argNotEmpty(reasons, "reasons");
    this.desiredDefinition = desiredDefinition;
    this.reasons = reasons;
    this.liveIndex = liveIndex;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.INDEX_ID, getIndexId())
        .field(Fields.TYPE, getType())
        .field(Fields.EXISTING_DEFINITIONS, existingDefinitions())
        .field(Fields.DESIRED_DEFINITION, this.desiredDefinition)
        .field(Fields.REASONS, this.reasons)
        .build();
  }

  abstract Type getType();

  abstract List<IndexDefinition> existingDefinitions();

  public IndexGeneration getLiveIndex() {
    return this.liveIndex;
  }

  public ObjectId getIndexId() {
    return this.liveIndex.getDefinition().getIndexId();
  }

  public IndexDefinitionGenerationProducer getDesiredDefinition() {
    return this.desiredDefinition;
  }

  public List<String> getReasons() {
    return this.reasons;
  }

  public DifferentFromLiveNoStaged asDifferentFromLiveNoStaged() {
    Check.instanceOf(this, DifferentFromLiveNoStaged.class);
    return (DifferentFromLiveNoStaged) this;
  }

  public SameAsLiveDifferentFromStaged asSameAsLiveDifferentFromStaged() {
    Check.instanceOf(this, SameAsLiveDifferentFromStaged.class);
    return (SameAsLiveDifferentFromStaged) this;
  }

  public DifferentFromBoth asDifferentFromBoth() {
    Check.instanceOf(this, DifferentFromBoth.class);
    return (DifferentFromBoth) this;
  }

  public static class DifferentFromLiveNoStaged extends ModifiedIndexInformation {

    DifferentFromLiveNoStaged(
        IndexDefinitionGenerationProducer desiredDefinition,
        List<String> reasons,
        IndexGeneration liveIndex) {
      super(desiredDefinition, reasons, liveIndex);
    }

    @Override
    public Type getType() {
      return Type.DIFFERENT_FROM_LIVE_NO_STAGED;
    }

    @Override
    protected List<IndexDefinition> existingDefinitions() {
      return List.of(getLiveIndex().getDefinition());
    }
  }

  public static class SameAsLiveDifferentFromStaged extends ModifiedIndexInformation {

    private final IndexGeneration staged;

    SameAsLiveDifferentFromStaged(
        IndexDefinitionGenerationProducer desiredDefinition,
        List<String> liveDifferences,
        IndexGeneration liveIndex,
        List<String> stagedDifferences,
        IndexGeneration staged) {
      super(desiredDefinition, mergeReasons(liveDifferences, stagedDifferences), liveIndex);
      this.staged = staged;
    }

    IndexGeneration getStaged() {
      return this.staged;
    }

    @Override
    public Type getType() {
      return Type.SAME_AS_LIVE_DIFFERENT_FROM_STAGED;
    }

    @Override
    protected List<IndexDefinition> existingDefinitions() {
      return List.of(getLiveIndex().getDefinition(), getStaged().getDefinition());
    }

    public IndexGeneration getStagedIndex() {
      return this.staged;
    }
  }

  public static class DifferentFromBoth extends ModifiedIndexInformation {

    private final IndexGeneration staged;

    DifferentFromBoth(
        IndexDefinitionGenerationProducer desiredDefinition,
        List<String> liveDifferences,
        IndexGeneration liveIndex,
        List<String> stagedDifferences,
        IndexGeneration staged) {
      super(desiredDefinition, mergeReasons(liveDifferences, stagedDifferences), liveIndex);
      this.staged = staged;
    }

    @Override
    public Type getType() {
      return Type.DIFFERENT_FROM_BOTH;
    }

    @Override
    protected List<IndexDefinition> existingDefinitions() {
      return List.of(getLiveIndex().getDefinition(), this.staged.getDefinition());
    }

    public IndexGeneration getStagedIndex() {
      return this.staged;
    }
  }

  private static List<String> mergeReasons(
      List<String> liveDifferences, List<String> stagedDifferences) {
    // we prepend "staged index" or "live index" to the modification reasons for debugging purposes.
    return Stream.concat(
            stagedDifferences.stream().map(reason -> String.format("staged index: %s", reason)),
            liveDifferences.stream().map(reason -> String.format("live index: %s", reason)))
        .collect(Collectors.toUnmodifiableList());
  }
}
