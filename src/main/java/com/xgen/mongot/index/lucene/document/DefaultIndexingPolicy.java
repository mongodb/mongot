package com.xgen.mongot.index.lucene.document;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.stored.StoredBuilder;
import com.xgen.mongot.index.ingestion.stored.StoredDocumentBuilder;
import com.xgen.mongot.index.lucene.document.block.EmbeddedDocumentBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBlockBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.index.lucene.document.single.IndexableFieldFactory;
import com.xgen.mongot.index.lucene.document.single.LuceneSearchIndexDocumentBuilder;
import com.xgen.mongot.index.lucene.document.single.LuceneVectorIndexDocumentBuilder;
import com.xgen.mongot.index.lucene.document.single.RootDocumentBuilder;
import com.xgen.mongot.index.version.IndexCapabilities;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.bson.types.ObjectId;

/**
 * Creates a {@link LuceneIndexingPolicy}, which in turn is able to instantiate {@link
 * DocumentBlockBuilder}s given the {@code byte[] id} of a given source document.
 */
public class DefaultIndexingPolicy {
  public static LuceneIndexingPolicy create(
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexingMetricsUpdater indexingMetricsUpdater,
      FeatureFlags featureFlags) {
    return resolver.indexDefinition.hasEmbeddedFields()
        ? EmbeddedIndexingPolicy.create(
            indexAnalyzer, resolver, indexingMetricsUpdater, featureFlags)
        : RootDocumentIndexingPolicy.create(
            indexAnalyzer, resolver, indexingMetricsUpdater, featureFlags);
  }

  public static LuceneIndexingPolicy create(
      VectorIndexDefinition indexDefinition,
      IndexCapabilities indexCapabilities,
      IndexingMetricsUpdater indexingMetricsUpdater,
      FeatureFlags featureFlags) {
    // Check if the index has a nested root (embedded vector fields)
    if (indexDefinition.getMappings().hasNestedRoot()) {
      return VectorEmbeddedIndexingPolicy.create(
          indexDefinition, indexCapabilities, indexingMetricsUpdater, featureFlags);
    }
    return VectorIndexDocumentIndexingPolicy.create(
        indexDefinition, indexCapabilities, indexingMetricsUpdater, featureFlags);
  }

  static class EmbeddedIndexingPolicy implements LuceneIndexingPolicy {
    private final RootDocumentIndexingPolicy rootDocumentIndexingPolicy;
    private final DocumentFieldDefinition mappings;
    private final IndexingMetricsUpdater indexingMetricsUpdater;

    private final Analyzer indexAnalyzer;
    private final SearchFieldDefinitionResolver resolver;

    EmbeddedIndexingPolicy(
        RootDocumentIndexingPolicy rootDocumentIndexingPolicy,
        DocumentFieldDefinition mappings,
        IndexingMetricsUpdater indexingMetricsUpdater,
        Analyzer indexAnalyzer,
        SearchFieldDefinitionResolver resolver) {
      this.rootDocumentIndexingPolicy = rootDocumentIndexingPolicy;
      this.mappings = mappings;
      this.indexingMetricsUpdater = indexingMetricsUpdater;
      this.indexAnalyzer = indexAnalyzer;
      this.resolver = resolver;
    }

    static EmbeddedIndexingPolicy create(
        Analyzer indexAnalyzer,
        SearchFieldDefinitionResolver resolver,
        IndexingMetricsUpdater indexingMetricsUpdater,
        FeatureFlags featureFlags) {
      return new EmbeddedIndexingPolicy(
          RootDocumentIndexingPolicy.create(
              indexAnalyzer, resolver, indexingMetricsUpdater, featureFlags),
          resolver.indexDefinition.getMappings(),
          indexingMetricsUpdater,
          indexAnalyzer,
          resolver);
    }

    @Override
    public DocumentBlockBuilder createBuilder(byte[] id) {
      return EmbeddedDocumentBuilder.createRoot(
          this.rootDocumentIndexingPolicy.createBuilder(id),
          this.mappings,
          this.indexingMetricsUpdater,
          this.indexAnalyzer,
          this.resolver,
          id);
    }
  }

  public static class RootDocumentIndexingPolicy implements LuceneIndexingPolicy {
    private final Optional<FacetsConfig> facetsConfig;
    private final DocumentFieldDefinition mappings;
    private final Supplier<Optional<StoredBuilder>> storedBuilderFactory;
    private final boolean isEmbedded;

    private final Analyzer indexAnalyzer;
    private final SearchFieldDefinitionResolver resolver;
    private final IndexingMetricsUpdater indexingMetricsUpdater;
    private final ObjectId indexId;
    private final FeatureFlags featureFlags;

    RootDocumentIndexingPolicy(
        ObjectId indexId,
        Optional<FacetsConfig> facetsConfig,
        DocumentFieldDefinition mappings,
        Supplier<Optional<StoredBuilder>> storedBuilderFactory,
        boolean isEmbedded,
        Analyzer indexAnalyzer,
        SearchFieldDefinitionResolver resolver,
        IndexingMetricsUpdater indexingMetricsUpdater,
        FeatureFlags featureFlags) {
      this.indexId = indexId;
      this.facetsConfig = facetsConfig;
      this.mappings = mappings;
      this.storedBuilderFactory = storedBuilderFactory;
      this.isEmbedded = isEmbedded;
      this.indexAnalyzer = indexAnalyzer;
      this.resolver = resolver;
      this.indexingMetricsUpdater = indexingMetricsUpdater;
      this.featureFlags = featureFlags;
    }

    public static RootDocumentIndexingPolicy create(
        Analyzer indexAnalyzer,
        SearchFieldDefinitionResolver resolver,
        IndexingMetricsUpdater indexingMetricsUpdater,
        FeatureFlags featureFlags) {
      return new RootDocumentIndexingPolicy(
          resolver.indexDefinition.getIndexId(),
          IndexableFieldFactory.createFacetsConfig(resolver.indexDefinition),
          resolver.indexDefinition.getMappings(),
          () -> StoredDocumentBuilder.create(resolver.indexDefinition.getStoredSource()),
          resolver.indexDefinition.hasEmbeddedFields(),
          indexAnalyzer,
          resolver,
          indexingMetricsUpdater,
          featureFlags);
    }

    @Override
    public RootDocumentBuilder createBuilder(byte[] id) {
      DocumentBuilder builder =
          this.isEmbedded
              ? LuceneSearchIndexDocumentBuilder.createEmbeddedRoot(
                  id,
                  this.mappings,
                  this.indexAnalyzer,
                  this.resolver,
                  this.indexingMetricsUpdater,
                  this.featureFlags)
              : LuceneSearchIndexDocumentBuilder.createRoot(
                  id,
                  this.mappings,
                  this.indexAnalyzer,
                  this.resolver,
                  this.indexingMetricsUpdater,
                  this.featureFlags);

      return RootDocumentBuilder.create(
          this.indexId,
          builder,
          this.storedBuilderFactory.get(),
          this.facetsConfig,
          this.resolver.getIndexCapabilities(),
          this.indexingMetricsUpdater);
    }
  }

  /**
   * {@link LuceneIndexingPolicy} for vector indexes that have a nested root (see {@link
   * VectorIndexDefinition#getNestedRoot()}).
   *
   * <p>When a vector index has a nested root, vector fields live under an array path (e.g. {@code
   * "sections"}). This policy creates {@link DocumentBlockBuilder}s that use {@link
   * com.xgen.mongot.index.lucene.document.block.VectorEmbeddedDocumentBuilder}, so each source BSON
   * document can produce multiple Lucene documents in a block: one root document plus one child
   * document per element of the array at the nested root.
   *
   * <p>When the index has no nested root, {@link
   * DefaultIndexingPolicy#create(VectorIndexDefinition, IndexCapabilities, IndexingMetricsUpdater,
   * FeatureFlags)} uses {@link VectorIndexDocumentIndexingPolicy} instead, which produces a single
   * Lucene document per source document.
   */
  static class VectorEmbeddedIndexingPolicy implements LuceneIndexingPolicy {
    private final VectorIndexDocumentIndexingPolicy rootDocumentIndexingPolicy;
    private final VectorIndexFieldMapping mapping;
    private final IndexCapabilities indexCapabilities;
    private final IndexingMetricsUpdater indexingMetricsUpdater;
    private final VectorIndexDefinition vectorIndexDefinition;
    private final FeatureFlags featureFlags;

    VectorEmbeddedIndexingPolicy(
        VectorIndexDocumentIndexingPolicy rootDocumentIndexingPolicy,
        VectorIndexFieldMapping mapping,
        IndexCapabilities indexCapabilities,
        IndexingMetricsUpdater indexingMetricsUpdater,
        VectorIndexDefinition vectorIndexDefinition,
        FeatureFlags featureFlags) {
      this.rootDocumentIndexingPolicy = rootDocumentIndexingPolicy;
      this.mapping = mapping;
      this.indexCapabilities = indexCapabilities;
      this.indexingMetricsUpdater = indexingMetricsUpdater;
      this.featureFlags = featureFlags;
      this.vectorIndexDefinition = vectorIndexDefinition;
    }

    static VectorEmbeddedIndexingPolicy create(
        VectorIndexDefinition indexDefinition,
        IndexCapabilities indexCapabilities,
        IndexingMetricsUpdater indexingMetricsUpdater,
        FeatureFlags featureFlags) {
      return new VectorEmbeddedIndexingPolicy(
          VectorIndexDocumentIndexingPolicy.create(
              indexDefinition, indexCapabilities, indexingMetricsUpdater, featureFlags),
          indexDefinition.getMappings(),
          indexCapabilities,
          indexingMetricsUpdater,
          indexDefinition,
          featureFlags);
    }

    @Override
    public DocumentBlockBuilder createBuilder(byte[] id) {
      return createBuilder(id, IndexingPolicyBuilderContext.builder().build());
    }

    @Override
    public DocumentBlockBuilder createBuilder(byte[] id, IndexingPolicyBuilderContext context) {
      LuceneVectorIndexDocumentBuilder rawBuilder =
          this.rootDocumentIndexingPolicy.createRawBuilder(id, context);

      Optional<StoredBuilder> storedBuilder =
          StoredDocumentBuilder.create(this.vectorIndexDefinition.getStoredSource());

      DocumentBuilder rootBuilder =
          RootDocumentBuilder.create(
              this.vectorIndexDefinition.getIndexId(),
              rawBuilder,
              storedBuilder,
              Optional.empty(),
              this.indexCapabilities,
              this.indexingMetricsUpdater);

      return com.xgen.mongot.index.lucene.document.block.VectorEmbeddedDocumentBuilder.createRoot(
          rootBuilder,
          this.mapping,
          id,
          this.indexingMetricsUpdater,
          this.indexCapabilities,
          context.autoEmbeddings(),
          this.featureFlags);
    }
  }

  public static class VectorIndexDocumentIndexingPolicy implements LuceneIndexingPolicy {
    private final VectorIndexDefinition indexDefinition;
    private final VectorIndexFieldMapping mapping;
    private final IndexCapabilities indexCapabilities;
    private final IndexingMetricsUpdater indexingMetricsUpdater;
    private final FeatureFlags featureFlags;

    VectorIndexDocumentIndexingPolicy(
        VectorIndexDefinition indexDefinition,
        VectorIndexFieldMapping mapping,
        IndexCapabilities indexCapabilities,
        IndexingMetricsUpdater indexingMetricsUpdater,
        FeatureFlags featureFlags) {
      this.indexDefinition = indexDefinition;
      this.mapping = mapping;
      this.indexCapabilities = indexCapabilities;
      this.indexingMetricsUpdater = indexingMetricsUpdater;
      this.featureFlags = featureFlags;
    }

    public static VectorIndexDocumentIndexingPolicy create(
        VectorIndexDefinition indexDefinition,
        IndexCapabilities indexCapabilities,
        IndexingMetricsUpdater indexingMetricsUpdater,
        FeatureFlags featureFlags) {
      return new VectorIndexDocumentIndexingPolicy(
          indexDefinition,
          indexDefinition.getMappings(),
          indexCapabilities,
          indexingMetricsUpdater,
          featureFlags);
    }

    @Override
    public DocumentBlockBuilder createBuilder(byte[] id) {
      return createBuilder(id, IndexingPolicyBuilderContext.builder().build());
    }

    @Override
    public DocumentBlockBuilder createBuilder(byte[] id, IndexingPolicyBuilderContext context) {
      LuceneVectorIndexDocumentBuilder builder = createRawBuilder(id, context);

      Optional<StoredBuilder> storedBuilder =
          StoredDocumentBuilder.create(this.indexDefinition.getStoredSource());

      return RootDocumentBuilder.create(
          this.indexDefinition.getIndexId(),
          builder,
          storedBuilder,
          Optional.empty(),
          this.indexCapabilities,
          this.indexingMetricsUpdater);
    }

    /**
     * Creates a raw {@link LuceneVectorIndexDocumentBuilder} without wrapping it in a {@link
     * RootDocumentBuilder}. This is used by {@link VectorEmbeddedIndexingPolicy} to get the raw
     * builder for embedded document handling.
     */
    LuceneVectorIndexDocumentBuilder createRawBuilder(
        byte[] id, IndexingPolicyBuilderContext context) {
      return LuceneVectorIndexDocumentBuilder.createRoot(
          id,
          this.mapping,
          this.indexCapabilities,
          this.indexingMetricsUpdater,
          context,
          this.featureFlags);
    }
  }
}
