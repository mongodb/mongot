package com.xgen.mongot.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.lucene.util.Version;

/**
 * Registers a gauge carrying mongot's own build version so a fleet can audit version / CVE.
 *
 * <p>Registered on both the MMS and community (self-managed) bootstrap paths so the metric matches
 * across deployments.
 */
public final class BuildInfoMetrics {

  private BuildInfoMetrics() {}

  public static void register(
      String mongotVersion, boolean indexFeatureVersionFourEnabled, MeterRegistry meterRegistry) {
    new MetricsFactory("build", meterRegistry)
        .numGauge(
            "version",
            Tags.of(
                "version", mongotVersion,
                "javaVersion", System.getProperty("java.version"),
                "luceneVersion", Version.LATEST.toString(),
                "indexFeatureVersionFourEnabled", String.valueOf(indexFeatureVersionFourEnabled)))
        .set(1);
  }
}
