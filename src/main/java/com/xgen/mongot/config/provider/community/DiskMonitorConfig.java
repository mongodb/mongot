package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

/**
 * Disk-usage thresholds, as a fraction of the data-path filesystem capacity, that gate replication
 * and crash the process before the disk fills entirely
 */
public record DiskMonitorConfig(
    double pauseReplicationThreshold,
    double resumeReplicationThreshold,
    double crashThreshold,
    double pauseInitialSyncThreshold,
    double resumeInitialSyncThreshold)
    implements DocumentEncodable {

  private static final double DEFAULT_PAUSE_REPLICATION_THRESHOLD = 0.90;
  private static final double DEFAULT_RESUME_REPLICATION_THRESHOLD = 0.85;
  private static final double DEFAULT_CRASH_THRESHOLD = 0.95;
  private static final double DEFAULT_PAUSE_INITIAL_SYNC_THRESHOLD = 0.85;
  private static final double DEFAULT_RESUME_INITIAL_SYNC_THRESHOLD = 0.80;

  public static DiskMonitorConfig getDefault() {
    return new DiskMonitorConfig(
        DEFAULT_PAUSE_REPLICATION_THRESHOLD,
        DEFAULT_RESUME_REPLICATION_THRESHOLD,
        DEFAULT_CRASH_THRESHOLD,
        DEFAULT_PAUSE_INITIAL_SYNC_THRESHOLD,
        DEFAULT_RESUME_INITIAL_SYNC_THRESHOLD);
  }

  public static DiskMonitorConfig fromBson(DocumentParser parser) throws BsonParseException {
    DiskMonitorConfig config =
        new DiskMonitorConfig(
            parser.getField(Fields.PAUSE_REPLICATION_THRESHOLD).unwrap(),
            parser.getField(Fields.RESUME_REPLICATION_THRESHOLD).unwrap(),
            parser.getField(Fields.CRASH_THRESHOLD).unwrap(),
            parser.getField(Fields.PAUSE_INITIAL_SYNC_THRESHOLD).unwrap(),
            parser.getField(Fields.RESUME_INITIAL_SYNC_THRESHOLD).unwrap());

    if (config.pauseReplicationThreshold() < config.resumeReplicationThreshold()) {
      parser
          .getContext()
          .handleSemanticError(
              "pauseReplicationThreshold must be greater than or equal to"
                  + " resumeReplicationThreshold");
    }

    if (config.pauseInitialSyncThreshold() < config.resumeInitialSyncThreshold()) {
      parser
          .getContext()
          .handleSemanticError(
              "pauseInitialSyncThreshold must be greater than or equal to"
                  + " resumeInitialSyncThreshold");
    }

    if (config.resumeInitialSyncThreshold() >= config.resumeReplicationThreshold()) {
      parser
          .getContext()
          .handleSemanticError(
              "resumeInitialSyncThreshold must be less than resumeReplicationThreshold");
    }

    return config;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PAUSE_REPLICATION_THRESHOLD, this.pauseReplicationThreshold)
        .field(Fields.RESUME_REPLICATION_THRESHOLD, this.resumeReplicationThreshold)
        .field(Fields.CRASH_THRESHOLD, this.crashThreshold)
        .field(Fields.PAUSE_INITIAL_SYNC_THRESHOLD, this.pauseInitialSyncThreshold)
        .field(Fields.RESUME_INITIAL_SYNC_THRESHOLD, this.resumeInitialSyncThreshold)
        .build();
  }

  private static class Fields {
    /** Above this, all replication (initial sync + change stream) is paused. */
    public static final Field.WithDefault<Double> PAUSE_REPLICATION_THRESHOLD =
        Field.builder("pauseReplicationThreshold")
            .doubleField()
            .mustBeWithinBounds(Range.of(0.0, 1.0))
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(DEFAULT_PAUSE_REPLICATION_THRESHOLD);

    /** Below this, paused replication resumes. */
    public static final Field.WithDefault<Double> RESUME_REPLICATION_THRESHOLD =
        Field.builder("resumeReplicationThreshold")
            .doubleField()
            .mustBeWithinBounds(Range.of(0.0, 1.0))
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(DEFAULT_RESUME_REPLICATION_THRESHOLD);

    /** At or above this, mongot crashes to avoid filling the disk entirely. */
    public static final Field.WithDefault<Double> CRASH_THRESHOLD =
        Field.builder("crashThreshold")
            .doubleField()
            .mustBeWithinBounds(Range.of(0.0, 1.0))
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(DEFAULT_CRASH_THRESHOLD);

    /** Above this, initial syncs are paused while change-stream replication continues. */
    public static final Field.WithDefault<Double> PAUSE_INITIAL_SYNC_THRESHOLD =
        Field.builder("pauseInitialSyncThreshold")
            .doubleField()
            .mustBeWithinBounds(Range.of(0.0, 1.0))
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(DEFAULT_PAUSE_INITIAL_SYNC_THRESHOLD);

    /** Below this, paused initial syncs resume. */
    public static final Field.WithDefault<Double> RESUME_INITIAL_SYNC_THRESHOLD =
        Field.builder("resumeInitialSyncThreshold")
            .doubleField()
            .mustBeWithinBounds(Range.of(0.0, 1.0))
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(DEFAULT_RESUME_INITIAL_SYNC_THRESHOLD);
  }
}
