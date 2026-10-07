package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.backup.JournalEntryIdentity;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class IncompatibleIndexesTest {

  private static final String DATABASE = "bar";
  private static final UUID COLLECTION_UUID =
      UUID.fromString("00000000-1111-2222-3333-444444444444");
  private static final UUID OTHER_COLLECTION_UUID =
      UUID.fromString("00000000-1111-2222-3333-555555555555");
  private static final ObjectId INDEX_ID = new ObjectId("012345678901234567890123");
  private static final ObjectId OTHER_INDEX_ID = new ObjectId("012345678901234567890124");

  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  @Test
  public void newRegistry_isEmpty() {
    var incompatibleIndexes = incompatibleIndexes();

    Assert.assertTrue(incompatibleIndexes.getLiveIndexes().isEmpty());
    Assert.assertTrue(incompatibleIndexes.getStagedIndexes().isEmpty());
  }

  @Test
  public void addLiveIndex_isTrackedAsLive() {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex live = quarantined(INDEX_ID, "foo", "field is required", 1);

    incompatibleIndexes.addLiveIndex(live);

    Assert.assertEquals(List.of(live), List.copyOf(incompatibleIndexes.getLiveIndexes()));
    Assert.assertTrue(incompatibleIndexes.getStagedIndexes().isEmpty());
  }

  @Test
  public void addStagedIndex_isTrackedAsStaged() {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex staged = quarantined(INDEX_ID, "foo", "field is required", 1);

    incompatibleIndexes.addStagedIndex(staged);

    Assert.assertEquals(List.of(staged), List.copyOf(incompatibleIndexes.getStagedIndexes()));
    Assert.assertTrue(incompatibleIndexes.getLiveIndexes().isEmpty());
  }

  /** An index can have a quarantined generation staged and live at once. */
  @Test
  public void sameIndexLiveAndStaged_keepsBothGenerations() {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex live = quarantined(INDEX_ID, "foo", "live is broken", 1);
    QuarantinedIndex staged = quarantined(INDEX_ID, "foo", "staged is broken", 2);

    incompatibleIndexes.addLiveIndex(live);
    incompatibleIndexes.addStagedIndex(staged);

    Assert.assertEquals(List.of(live), List.copyOf(incompatibleIndexes.getLiveIndexes()));
    Assert.assertEquals(List.of(staged), List.copyOf(incompatibleIndexes.getStagedIndexes()));
  }

  @Test
  public void differentIndexes_areTrackedIndependently() {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex first = quarantined(INDEX_ID, "foo", "foo is broken", 1);
    QuarantinedIndex second = quarantined(OTHER_INDEX_ID, "other", "other is broken", 1);

    incompatibleIndexes.addLiveIndex(first);
    incompatibleIndexes.addLiveIndex(second);

    Assert.assertEquals(2, incompatibleIndexes.getLiveIndexes().size());
    Assert.assertTrue(incompatibleIndexes.getLiveIndexes().contains(first));
    Assert.assertTrue(incompatibleIndexes.getLiveIndexes().contains(second));
  }

  @Test
  public void addLiveIndex_sameIndexTwice_replacesTheEarlierGeneration() {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex second = quarantined(INDEX_ID, "foo", "second", 2);

    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "first", 1));
    incompatibleIndexes.addLiveIndex(second);

    Assert.assertEquals(List.of(second), List.copyOf(incompatibleIndexes.getLiveIndexes()));
  }

  @Test
  public void addStagedIndex_sameIndexTwice_replacesTheEarlierGeneration() {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex second = quarantined(INDEX_ID, "foo", "second", 2);

    incompatibleIndexes.addStagedIndex(quarantined(INDEX_ID, "foo", "first", 1));
    incompatibleIndexes.addStagedIndex(second);

    Assert.assertEquals(List.of(second), List.copyOf(incompatibleIndexes.getStagedIndexes()));
  }

  @Test
  public void throwIfQuarantined_matchingLiveIndex_throwsWithTheParseError() {
    var incompatibleIndexes = incompatibleIndexes();
    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "field is required", 1));

    IndexUnavailableException thrown =
        Assert.assertThrows(
            IndexUnavailableException.class,
            () -> incompatibleIndexes.throwIfQuarantined(DATABASE, COLLECTION_UUID, "foo"));

    Assert.assertEquals(
        "Index 'foo' is unavailable: its definition cannot be parsed (field is required).",
        thrown.getMessage());
  }

  @Test
  public void throwIfQuarantined_nonMatchingProperties_doesNothing() throws Exception {
    var incompatibleIndexes = incompatibleIndexes();
    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "field is required", 1));

    incompatibleIndexes.throwIfQuarantined(DATABASE, COLLECTION_UUID, "notFoo");
    incompatibleIndexes.throwIfQuarantined("otherDatabase", COLLECTION_UUID, "foo");
    incompatibleIndexes.throwIfQuarantined(DATABASE, OTHER_COLLECTION_UUID, "foo");
  }

  @Test
  public void throwIfQuarantined_nothingQuarantined_doesNothing() throws Exception {
    incompatibleIndexes().throwIfQuarantined(DATABASE, COLLECTION_UUID, "foo");
  }

  /**
   * A staged quarantine has a parseable live counterpart, which queries resolve as they always do.
   */
  @Test
  public void throwIfQuarantined_stagedQuarantineOnly_doesNothing() throws Exception {
    var incompatibleIndexes = incompatibleIndexes();

    incompatibleIndexes.addStagedIndex(quarantined(INDEX_ID, "foo", "staged is broken", 2));

    incompatibleIndexes.throwIfQuarantined(DATABASE, COLLECTION_UUID, "foo");
  }

  @Test
  public void addLiveIndex_replacingARenamedIndex_leavesNoStaleLookup() throws Exception {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex renamed = quarantined(INDEX_ID, "newName", "second", 2);

    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "oldName", "first", 1));
    incompatibleIndexes.addLiveIndex(renamed);

    incompatibleIndexes.throwIfQuarantined(DATABASE, COLLECTION_UUID, "oldName");
    Assert.assertThrows(
        IndexUnavailableException.class,
        () -> incompatibleIndexes.throwIfQuarantined(DATABASE, COLLECTION_UUID, "newName"));
    Assert.assertEquals(List.of(renamed), List.copyOf(incompatibleIndexes.getLiveIndexes()));
  }

  @Test
  public void removeLiveIndex_removesFromEveryLookup() throws Exception {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex live = quarantined(INDEX_ID, "foo", "foo is broken", 1);
    QuarantinedIndex other = quarantined(OTHER_INDEX_ID, "other", "other is broken", 1);
    incompatibleIndexes.addLiveIndex(live);
    incompatibleIndexes.addLiveIndex(other);

    Assert.assertEquals(Optional.of(live), incompatibleIndexes.removeLiveIndex(INDEX_ID));

    incompatibleIndexes.throwIfQuarantined(DATABASE, COLLECTION_UUID, "foo");
    Assert.assertEquals(List.of(other), List.copyOf(incompatibleIndexes.getLiveIndexes()));
  }

  @Test
  public void removeLiveIndex_untrackedIndex_isEmpty() {
    var incompatibleIndexes = incompatibleIndexes();
    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "foo is broken", 1));

    Assert.assertEquals(Optional.empty(), incompatibleIndexes.removeLiveIndex(OTHER_INDEX_ID));

    Assert.assertEquals(1, incompatibleIndexes.getLiveIndexes().size());
  }

  /** Removing the live generation leaves the staged one, which is tracked separately. */
  @Test
  public void removeLiveIndex_doesNotRemoveTheStagedGeneration() {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex staged = quarantined(INDEX_ID, "foo", "staged is broken", 2);
    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "live is broken", 1));
    incompatibleIndexes.addStagedIndex(staged);

    incompatibleIndexes.removeLiveIndex(INDEX_ID);

    Assert.assertTrue(incompatibleIndexes.getLiveIndexes().isEmpty());
    Assert.assertEquals(List.of(staged), List.copyOf(incompatibleIndexes.getStagedIndexes()));
  }

  @Test
  public void newRegistry_reportsEmpty() {
    Assert.assertTrue(incompatibleIndexes().isEmpty());
  }

  @Test
  public void removeAllExcept_dropsTheIdsNotKept_andReportsThem() {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex kept = quarantined(INDEX_ID, "foo", "foo is broken", 1);
    QuarantinedIndex removed = quarantined(OTHER_INDEX_ID, "other", "other is broken", 1);
    incompatibleIndexes.addLiveIndex(kept);
    incompatibleIndexes.addLiveIndex(removed);

    List<QuarantinedIndex> dropped = incompatibleIndexes.removeAllExcept(Set.of(INDEX_ID));

    Assert.assertEquals(List.of(removed), dropped);
    Assert.assertEquals(List.of(kept), List.copyOf(incompatibleIndexes.getLiveIndexes()));
  }

  /** Both generations of an index go together: the keep set is by index id, not by generation. */
  @Test
  public void removeAllExcept_dropsStagedAndLiveGenerationsOfTheSameIndex() {
    var incompatibleIndexes = incompatibleIndexes();
    QuarantinedIndex live = quarantined(INDEX_ID, "foo", "live is broken", 1);
    QuarantinedIndex staged = quarantined(INDEX_ID, "foo", "staged is broken", 2);
    incompatibleIndexes.addLiveIndex(live);
    incompatibleIndexes.addStagedIndex(staged);

    List<QuarantinedIndex> dropped = incompatibleIndexes.removeAllExcept(Set.of());

    Assert.assertEquals(Set.of(live, staged), Set.copyOf(dropped));
    Assert.assertTrue(incompatibleIndexes.isEmpty());
  }

  @Test
  public void removeAllExcept_keepingEverything_removesNothing() {
    var incompatibleIndexes = incompatibleIndexes();
    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "foo is broken", 1));

    Assert.assertTrue(incompatibleIndexes.removeAllExcept(Set.of(INDEX_ID)).isEmpty());
    Assert.assertFalse(incompatibleIndexes.isEmpty());
  }

  @Test
  public void addLiveIndex_registersOneQuarantinedGaugeAtOne() {
    var incompatibleIndexes = incompatibleIndexes();

    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "field is required", 1));

    List<Gauge> gauges = quarantinedStatusGauges();
    Assert.assertEquals(1, gauges.size());
    Assert.assertEquals(fullMetricName(), gauges.get(0).getId().getName());
    Assert.assertEquals(expectedGaugeTags(INDEX_ID), Tags.of(gauges.get(0).getId().getTags()));
    Assert.assertEquals(1, gauges.get(0).value(), 0.0);
  }

  @Test
  public void removeLiveIndex_removesItsQuarantinedGauge() {
    var incompatibleIndexes = incompatibleIndexes();
    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "field is required", 1));

    incompatibleIndexes.removeLiveIndex(INDEX_ID);

    Assert.assertTrue(quarantinedStatusGauges().isEmpty());
  }

  @Test
  public void removeAllExcept_dropsGaugesForRemovedLiveIndexes() {
    var incompatibleIndexes = incompatibleIndexes();
    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "kept", 1));
    incompatibleIndexes.addLiveIndex(quarantined(OTHER_INDEX_ID, "other", "removed", 1));

    incompatibleIndexes.removeAllExcept(Set.of(INDEX_ID));

    List<Gauge> gauges = quarantinedStatusGauges();
    Assert.assertEquals(1, gauges.size());
    Assert.assertEquals(expectedGaugeTags(INDEX_ID), Tags.of(gauges.get(0).getId().getTags()));
  }

  @Test
  public void addStagedIndex_registersNoGauge() {
    var incompatibleIndexes = incompatibleIndexes();

    incompatibleIndexes.addStagedIndex(quarantined(INDEX_ID, "foo", "staged is broken", 2));

    Assert.assertTrue(quarantinedStatusGauges().isEmpty());
  }

  /** Both generations of the same index share one gauge, for the live one that affects serving. */
  @Test
  public void sameIndexLiveAndStaged_registersOneGauge() {
    var incompatibleIndexes = incompatibleIndexes();
    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "live is broken", 1));
    incompatibleIndexes.addStagedIndex(quarantined(INDEX_ID, "foo", "staged is broken", 2));

    List<Gauge> gauges = quarantinedStatusGauges();

    Assert.assertEquals(1, gauges.size());
    Assert.assertEquals(expectedGaugeTags(INDEX_ID), Tags.of(gauges.get(0).getId().getTags()));
  }

  /**
   * The gauge carries no generation id, so re-adding the same index id must overwrite the existing
   * series instead of stranding it.
   */
  @Test
  public void addLiveIndex_sameIndexTwice_registersOneGauge() {
    var incompatibleIndexes = incompatibleIndexes();

    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "first", 1));
    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "second", 2));

    List<Gauge> gauges = quarantinedStatusGauges();
    Assert.assertEquals(1, gauges.size());
    Assert.assertEquals(expectedGaugeTags(INDEX_ID), Tags.of(gauges.get(0).getId().getTags()));
  }

  /**
   * Prometheus requires every series of a metric name to share one tag-key set, and the per-index
   * status series carry {@code generationId logString} that a quarantine cannot. Registering the
   * quarantine gauge under the status metric name would make one of the two invisible; this pins
   * that they coexist on a real {@link PrometheusMeterRegistry}, which a SimpleMeterRegistry would
   * not catch.
   */
  @Test
  public void prometheusRegistry_quarantineGaugeCoexistsWithPerIndexStatusGauge() {
    var prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var statusMetricsFactory =
        new MetricsFactory(IndexMetricsUpdater.NAMESPACE, prometheusRegistry);
    // A per-index status series with the tag-key set the real gauges use, registered first to match
    // the startup order in which normal indexes precede quarantined ones.
    statusMetricsFactory.objectValueGauge(
        "indexStatusCode",
        new Object(),
        state -> 1.0,
        Tags.of(
            "generationId logString",
            "gen",
            "indexId logString",
            INDEX_ID.toHexString(),
            "status",
            "STEADY",
            "indexType",
            "search"));
    var incompatibleIndexes = new IncompatibleIndexes(prometheusRegistry);

    incompatibleIndexes.addLiveIndex(quarantined(INDEX_ID, "foo", "field is required", 1));

    String scrape = prometheusRegistry.scrape();
    Assert.assertTrue(scrape.contains("indexStatusCode"));
    Assert.assertTrue(scrape.contains("quarantinedIndex"));
  }

  private IncompatibleIndexes incompatibleIndexes() {
    return new IncompatibleIndexes(this.meterRegistry);
  }

  private List<Gauge> quarantinedStatusGauges() {
    return this.meterRegistry.getMeters().stream()
        .filter(meter -> meter.getId().getType() == Meter.Type.GAUGE)
        .map(meter -> (Gauge) meter)
        .filter(gauge -> gauge.getId().getName().equals(fullMetricName()))
        .filter(gauge -> "QUARANTINED".equals(gauge.getId().getTag("status")))
        .collect(Collectors.toList());
  }

  private static String fullMetricName() {
    return IndexMetricsUpdater.NAMESPACE + "." + IncompatibleIndexes.QUARANTINED_INDEX_METRIC_NAME;
  }

  private static Tags expectedGaugeTags(ObjectId indexId) {
    return Tags.of("status", "QUARANTINED")
        .and(PerIndexMetricsFactory.indexIdTag(indexId.toHexString()));
  }

  private static QuarantinedIndex quarantined(
      ObjectId indexId, String name, String parseError, int userIndexVersion) {
    var identity =
        new JournalEntryIdentity(
            indexId,
            DATABASE,
            COLLECTION_UUID,
            name,
            new Generation(new UserIndexVersion(userIndexVersion), IndexFormatVersion.CURRENT));

    return new QuarantinedIndex(
        identity,
        IndexDefinitionGeneration.Type.SEARCH,
        parseError,
        new BsonDocument("fieldFromTheFuture", new BsonBoolean(true)));
  }
}
