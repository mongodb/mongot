package com.xgen.mongot.index.query.optimizer;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import org.apache.lucene.index.IndexReader;

/** Per-query bundle of context passed to every rule visit. */
public record RewriteContext(
    IndexReader indexReader,
    QueryFactoryContext factoryContext,
    DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
    IndexMetricsUpdater.QueryingMetricsUpdater metrics,
    RewriteAudit audit) {}
