package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.PermissiveBsonParseContext;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.junit.Test;

public class DiskMonitorConfigTest {

  private static DiskMonitorConfig parse(BsonDocument doc) throws BsonParseException {
    PermissiveBsonParseContext context =
        PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY));
    try (var parser = BsonDocumentParser.withContext(context, doc).build()) {
      return DiskMonitorConfig.fromBson(parser);
    }
  }

  @Test
  public void roundTrip_default_preservesAllFields() throws BsonParseException {
    DiskMonitorConfig original = DiskMonitorConfig.getDefault();
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void roundTrip_full_preservesAllFields() throws BsonParseException {
    DiskMonitorConfig original = new DiskMonitorConfig(0.92, 0.87, 0.97, 0.82, 0.77);
    assertEquals(original, parse(original.toBson()));
  }

  @Test
  public void parse_emptyDocument_returnsDefaults() throws BsonParseException {
    DiskMonitorConfig parsed = parse(new BsonDocument());
    assertEquals(0.90, parsed.pauseReplicationThreshold(), 0.0);
    assertEquals(0.85, parsed.resumeReplicationThreshold(), 0.0);
    assertEquals(0.95, parsed.crashThreshold(), 0.0);
    assertEquals(0.85, parsed.pauseInitialSyncThreshold(), 0.0);
    assertEquals(0.80, parsed.resumeInitialSyncThreshold(), 0.0);
  }

  @Test
  public void parse_partialOverride_keepsDefaultsForOtherFields() throws BsonParseException {
    DiskMonitorConfig parsed = parse(new BsonDocument("crashThreshold", new BsonDouble(0.99)));
    assertEquals(0.99, parsed.crashThreshold(), 0.0);
    assertEquals(0.90, parsed.pauseReplicationThreshold(), 0.0);
    assertEquals(0.85, parsed.resumeReplicationThreshold(), 0.0);
    assertEquals(0.85, parsed.pauseInitialSyncThreshold(), 0.0);
    assertEquals(0.80, parsed.resumeInitialSyncThreshold(), 0.0);
  }

  @Test
  public void parse_exposesAllFields() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("pauseReplicationThreshold", new BsonDouble(0.92))
            .append("resumeReplicationThreshold", new BsonDouble(0.87))
            .append("crashThreshold", new BsonDouble(0.97))
            .append("pauseInitialSyncThreshold", new BsonDouble(0.82))
            .append("resumeInitialSyncThreshold", new BsonDouble(0.77));
    DiskMonitorConfig parsed = parse(doc);
    assertEquals(0.92, parsed.pauseReplicationThreshold(), 0.0);
    assertEquals(0.87, parsed.resumeReplicationThreshold(), 0.0);
    assertEquals(0.97, parsed.crashThreshold(), 0.0);
    assertEquals(0.82, parsed.pauseInitialSyncThreshold(), 0.0);
    assertEquals(0.77, parsed.resumeInitialSyncThreshold(), 0.0);
  }

  @Test
  public void parse_pauseReplicationEqualToResume_isAllowed() throws BsonParseException {
    BsonDocument doc =
        new BsonDocument()
            .append("pauseReplicationThreshold", new BsonDouble(0.90))
            .append("resumeReplicationThreshold", new BsonDouble(0.90));
    DiskMonitorConfig parsed = parse(doc);
    assertEquals(0.90, parsed.pauseReplicationThreshold(), 0.0);
    assertEquals(0.90, parsed.resumeReplicationThreshold(), 0.0);
  }

  @Test
  public void parse_pauseReplicationBelowResume_throws() {
    BsonDocument doc =
        new BsonDocument()
            .append("pauseReplicationThreshold", new BsonDouble(0.85))
            .append("resumeReplicationThreshold", new BsonDouble(0.90));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_pauseInitialSyncBelowResume_throws() {
    BsonDocument doc =
        new BsonDocument()
            .append("pauseInitialSyncThreshold", new BsonDouble(0.70))
            .append("resumeInitialSyncThreshold", new BsonDouble(0.80));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_resumeInitialSyncNotBelowResumeReplication_throws() {
    BsonDocument doc =
        new BsonDocument()
            .append("pauseInitialSyncThreshold", new BsonDouble(0.92))
            .append("resumeInitialSyncThreshold", new BsonDouble(0.91));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }

  @Test
  public void parse_thresholdOutOfBounds_throws() {
    BsonDocument doc = new BsonDocument("crashThreshold", new BsonDouble(1.01));
    assertThrows(BsonParseException.class, () -> parse(doc));
  }
}
