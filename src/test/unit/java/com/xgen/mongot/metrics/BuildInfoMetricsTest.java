package com.xgen.mongot.metrics;

import static org.junit.Assert.assertEquals;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class BuildInfoMetricsTest {

  @Test
  public void registersVersionGauge() {
    var meterRegistry = new SimpleMeterRegistry();
    BuildInfoMetrics.register("1.70.3", true, meterRegistry);

    // Metric name and tags mirror the existing MMS `build.version` gauge so fleet queries match.
    var gauge = meterRegistry.get("build.version").tags("version", "1.70.3").gauge();
    assertEquals(1.0, gauge.value(), 0.0);
    assertEquals("1.70.3", gauge.getId().getTag("version"));
    assertEquals(System.getProperty("java.version"), gauge.getId().getTag("javaVersion"));
    assertEquals(Version.LATEST.toString(), gauge.getId().getTag("luceneVersion"));
    assertEquals("true", gauge.getId().getTag("indexFeatureVersionFourEnabled"));
  }

  @Test
  public void propagatesToAttachedPrometheusChild() {
    // Production Community registers on a CompositeMeterRegistry with a Prometheus child attached.
    // Verify a gauge registered on the composite is visible to the attached Prometheus registry.
    var composite = new CompositeMeterRegistry();
    var prometheusChild = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    composite.add(prometheusChild);

    BuildInfoMetrics.register("1.70.3", false, composite);

    var gauge = prometheusChild.get("build.version").tags("version", "1.70.3").gauge();
    assertEquals(1.0, gauge.value(), 0.0);
  }
}
