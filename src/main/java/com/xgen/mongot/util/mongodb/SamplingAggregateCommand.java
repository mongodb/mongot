package com.xgen.mongot.util.mongodb;

import com.mongodb.client.model.Aggregates;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.serialization.AggregateCommandProxy;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

public class SamplingAggregateCommand {

  private final String collectionName;
  private final Long sampleLimit;
  private final OptionalLong batchSize;
  private final Optional<List<Bson>> viewDefinedStages;
  private final Optional<Bson> projectStage;
  private final OptionalLong maxTimeMs;

  public SamplingAggregateCommand(
      String collectionName,
      Long sampleLimit,
      OptionalLong batchSize,
      Optional<List<Bson>> viewDefinedStages,
      Optional<Bson> projectStage,
      OptionalLong maxTimeMs) {
    this.collectionName = collectionName;
    this.sampleLimit = sampleLimit;
    this.batchSize = batchSize;
    this.viewDefinedStages = viewDefinedStages;
    this.projectStage = projectStage;
    this.maxTimeMs = maxTimeMs;
  }

  public static class Builder {

    private Optional<String> collectionName = Optional.empty();
    private Optional<Long> sampleLimit = Optional.empty();
    private OptionalLong batchSize = OptionalLong.empty();
    private Optional<Bson> projectStage = Optional.empty();
    private Optional<List<Bson>> viewDefinedStages = Optional.empty();
    private OptionalLong maxTimeMs = OptionalLong.empty();

    /** Builds the configured ChangeStreamAggregateCommand. */
    public SamplingAggregateCommand build() {
      var collection = Check.isPresent(this.collectionName, "collectionName");
      var limit = Check.isPresent(this.sampleLimit, "sampleLimit");
      return new SamplingAggregateCommand(
          collection,
          limit,
          this.batchSize,
          this.viewDefinedStages,
          this.projectStage,
          this.maxTimeMs);
    }

    /** Sets the collection name. */
    public SamplingAggregateCommand.Builder collection(String collectionName) {
      this.collectionName = Optional.of(collectionName);
      return this;
    }

    /** Sets the sample size. */
    public SamplingAggregateCommand.Builder sampleLimit(long sampleLimit) {
      this.sampleLimit = Optional.of(sampleLimit);
      return this;
    }

    /** Sets the batch size. */
    public SamplingAggregateCommand.Builder batchSize(long batchSize) {
      this.batchSize = OptionalLong.of(batchSize);
      return this;
    }

    /** Sets the view-defined stages, only present when index is created on a view */
    public SamplingAggregateCommand.Builder viewDefinedStages(List<Bson> viewDefinedStages) {
      this.viewDefinedStages = Optional.of(viewDefinedStages);
      return this;
    }

    /** Sets the $project stage. */
    public SamplingAggregateCommand.Builder projectStage(BsonDocument projectStage) {
      this.projectStage = Optional.of(projectStage);
      return this;
    }

    /** Sets the maxTimeMS budget for the sampling query. */
    public SamplingAggregateCommand.Builder maxTimeMs(long maxTimeMs) {
      this.maxTimeMs = OptionalLong.of(maxTimeMs);
      return this;
    }
  }

  /** Constructs the proper AggregateCommandProxy for the SampleAggregationCommand. */
  public AggregateCommandProxy toProxy() {

    BsonValue aggregate = new BsonString(this.collectionName);
    BsonDocument sampleStage = Aggregates.sample(this.sampleLimit.intValue()).toBsonDocument();

    List<Bson> pipeline =
        new AggregationPipelineBuilder()
            // $sample first so MongoDB can use the fast pseudo-random sampling path
            .addStage(sampleStage)
            // apply view-defined $addFields/$set only on the sampled documents
            .addMultipleStages(this.viewDefinedStages)
            // run $project to include only required fields
            .addStage(this.projectStage)
            .build();

    Optional<Long> maxTimeMsOpt =
        this.maxTimeMs.isPresent() ? Optional.of(this.maxTimeMs.getAsLong()) : Optional.empty();

    return new AggregateCommandProxy(
        aggregate,
        pipeline,
        new AggregateCommandProxy.CursorProxy(this.batchSize),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        maxTimeMsOpt);
  }
}
