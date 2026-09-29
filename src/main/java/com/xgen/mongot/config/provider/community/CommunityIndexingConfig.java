package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.config.provider.community.CommunityIndexingConfig.LuceneConfig.MergePolicyConfig.TieredMergePolicyConfig.TieredMergePolicyConfigCliOptionRegistry;
import com.xgen.mongot.config.provider.community.CommunityIndexingConfig.LuceneConfig.MergePolicyConfig.TieredMergePolicyConfig.VectorMergePolicyConfig.VectorMergePolicyConfigCliOptionRegistry;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;
import picocli.CommandLine;

/** The {@code indexing} block of the Community mongot config file. */
public record CommunityIndexingConfig(
    Optional<LuceneConfig> luceneConfig, Optional<IndexDefinitionConfig> definitionConfig)
    implements DocumentEncodable {

  static class Fields {
    public static final Field.Optional<LuceneConfig> LUCENE_CONFIG =
        Field.builder("lucene")
            .classField(LuceneConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<IndexDefinitionConfig> DEFINITION_CONFIG =
        Field.builder("definition")
            .classField(IndexDefinitionConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public static final class CommunityIndexingConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Mixin
    public LuceneConfig.LuceneConfigCliOptionRegistry luceneConfigCliOptionRegistry =
        new LuceneConfig.LuceneConfigCliOptionRegistry();

    public CommunityIndexingConfigCliOptionRegistry() {
      addChildRegistry(Fields.LUCENE_CONFIG, this.luceneConfigCliOptionRegistry);
    }
  }

  public static CommunityIndexingConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new CommunityIndexingConfig(
        parser.getField(Fields.LUCENE_CONFIG).unwrap(),
        parser.getField(Fields.DEFINITION_CONFIG).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.LUCENE_CONFIG, this.luceneConfig)
        .field(Fields.DEFINITION_CONFIG, this.definitionConfig)
        .build();
  }

  public record LuceneConfig(
      Optional<RefreshConfig> refreshConfig,
      Optional<MergePolicyConfig> mergePolicyConfig,
      Optional<MergeSchedulerConfig> mergeSchedulerConfig,
      Optional<Integer> fieldLimit)
      implements DocumentEncodable {

    static class Fields {
      public static final Field.Optional<RefreshConfig> REFRESH_CONFIG =
          Field.builder("refresh")
              .classField(RefreshConfig::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();

      public static final Field.Optional<MergePolicyConfig> MERGE_POLICY_CONFIG =
          Field.builder("mergePolicy")
              .classField(MergePolicyConfig::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();

      public static final Field.Optional<MergeSchedulerConfig> MERGE_SCHEDULER_CONFIG =
          Field.builder("mergeScheduler")
              .classField(MergeSchedulerConfig::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> FIELD_LIMIT =
          Field.builder("fieldLimit")
              .intField()
              .mustBePositive()
              .mustBeWithinBounds(
                  Range.of(
                      com.xgen.mongot.index.lucene.config.LuceneConfig.MIN_FIELD_LIMIT,
                      Integer.MAX_VALUE))
              .optional()
              .noDefault();
    }

    public static final class LuceneConfigCliOptionRegistry extends CliOptionRegistry {
      @CommandLine.Option(
          names = "--indexingLuceneFieldLimit",
          description = "Maximum number of indexed fields allowed in an index")
      public Optional<Integer> fieldLimit = Optional.empty();

      @CommandLine.Mixin
      public RefreshConfig.RefreshConfigCliOptionRegistry refreshConfigCliOptionRegistry =
          new RefreshConfig.RefreshConfigCliOptionRegistry();

      @CommandLine.Mixin
      public MergePolicyConfig.MergePolicyConfigCliOptionRegistry
          mergePolicyConfigCliOptionRegistry =
              new MergePolicyConfig.MergePolicyConfigCliOptionRegistry();

      @CommandLine.Mixin
      public MergeSchedulerConfig.MergeSchedulerConfigCliOptionRegistry
          mergeSchedulerConfigCliOptionRegistry =
              new MergeSchedulerConfig.MergeSchedulerConfigCliOptionRegistry();

      public LuceneConfigCliOptionRegistry() {
        addChildRegistry(Fields.REFRESH_CONFIG, this.refreshConfigCliOptionRegistry);
        addChildRegistry(Fields.MERGE_POLICY_CONFIG, this.mergePolicyConfigCliOptionRegistry);
        addChildRegistry(Fields.MERGE_SCHEDULER_CONFIG, this.mergeSchedulerConfigCliOptionRegistry);
        addIntegerLeafValue(LuceneConfig.Fields.FIELD_LIMIT, () -> this.fieldLimit);
      }
    }

    public static LuceneConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new LuceneConfig(
          parser.getField(Fields.REFRESH_CONFIG).unwrap(),
          parser.getField(Fields.MERGE_POLICY_CONFIG).unwrap(),
          parser.getField(Fields.MERGE_SCHEDULER_CONFIG).unwrap(),
          parser.getField(Fields.FIELD_LIMIT).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.REFRESH_CONFIG, this.refreshConfig)
          .field(Fields.MERGE_POLICY_CONFIG, this.mergePolicyConfig)
          .field(Fields.MERGE_SCHEDULER_CONFIG, this.mergeSchedulerConfig)
          .field(Fields.FIELD_LIMIT, this.fieldLimit)
          .build();
    }

    public record RefreshConfig(Optional<Integer> intervalMs) implements DocumentEncodable {

      static class Fields {
        public static final Field.Optional<Integer> INTERVAL_MS =
            Field.builder("intervalMs").intField().mustBePositive().optional().noDefault();
      }

      public static final class RefreshConfigCliOptionRegistry extends CliOptionRegistry {
        @CommandLine.Option(
            names = "--indexingLuceneRefreshIntervalMs",
            description =
                "Interval that mongot waits before refreshing an index to make recently indexed"
                    + " changes visible to queries.")
        public Optional<Integer> intervalMs = Optional.empty();

        public RefreshConfigCliOptionRegistry() {
          addIntegerLeafValue(LuceneConfig.RefreshConfig.Fields.INTERVAL_MS, () -> this.intervalMs);
        }
      }

      public static RefreshConfig fromBson(DocumentParser parser) throws BsonParseException {
        return new RefreshConfig(parser.getField(Fields.INTERVAL_MS).unwrap());
      }

      @Override
      public BsonDocument toBson() {
        return BsonDocumentBuilder.builder().field(Fields.INTERVAL_MS, this.intervalMs).build();
      }
    }

    public record MergePolicyConfig(Optional<TieredMergePolicyConfig> tieredMergePolicyConfig)
        implements DocumentEncodable {

      static class Fields {
        public static final Field.Optional<TieredMergePolicyConfig> TIERED_MERGE_POLICY_CONFIG =
            Field.builder("tiered")
                .classField(TieredMergePolicyConfig::fromBson)
                .disallowUnknownFields()
                .optional()
                .noDefault();
      }

      public static final class MergePolicyConfigCliOptionRegistry extends CliOptionRegistry {
        @CommandLine.Mixin
        public TieredMergePolicyConfigCliOptionRegistry tieredMergePolicyConfigCliOptionRegistry =
            new TieredMergePolicyConfigCliOptionRegistry();

        public MergePolicyConfigCliOptionRegistry() {
          addChildRegistry(
              Fields.TIERED_MERGE_POLICY_CONFIG, this.tieredMergePolicyConfigCliOptionRegistry);
        }
      }

      public static MergePolicyConfig fromBson(DocumentParser parser) throws BsonParseException {
        return new MergePolicyConfig(parser.getField(Fields.TIERED_MERGE_POLICY_CONFIG).unwrap());
      }

      @Override
      public BsonDocument toBson() {
        return BsonDocumentBuilder.builder()
            .field(Fields.TIERED_MERGE_POLICY_CONFIG, this.tieredMergePolicyConfig)
            .build();
      }

      public record TieredMergePolicyConfig(
          Optional<VectorMergePolicyConfig> vectorMergePolicyConfig) implements DocumentEncodable {

        static class Fields {
          public static final Field.Optional<VectorMergePolicyConfig> VECTOR_MERGE_POLICY_CONFIG =
              Field.builder("vectorMergePolicy")
                  .classField(VectorMergePolicyConfig::fromBson)
                  .disallowUnknownFields()
                  .optional()
                  .noDefault();
        }

        public static TieredMergePolicyConfig fromBson(DocumentParser parser)
            throws BsonParseException {
          return new TieredMergePolicyConfig(
              parser.getField(Fields.VECTOR_MERGE_POLICY_CONFIG).unwrap());
        }

        @Override
        public BsonDocument toBson() {
          return BsonDocumentBuilder.builder()
              .field(Fields.VECTOR_MERGE_POLICY_CONFIG, this.vectorMergePolicyConfig)
              .build();
        }

        public static final class TieredMergePolicyConfigCliOptionRegistry
            extends CliOptionRegistry {
          @CommandLine.Mixin
          public VectorMergePolicyConfigCliOptionRegistry vectorMergePolicyConfigCliOptionRegistry =
              new VectorMergePolicyConfigCliOptionRegistry();

          public TieredMergePolicyConfigCliOptionRegistry() {
            addChildRegistry(
                Fields.VECTOR_MERGE_POLICY_CONFIG, this.vectorMergePolicyConfigCliOptionRegistry);
          }
        }

        public record VectorMergePolicyConfig(Optional<Integer> mergeBudgetMb)
            implements DocumentEncodable {

          static class Fields {
            public static final Field.Optional<Integer> MERGE_BUDGET_MB =
                Field.builder("mergeBudgetMb").intField().mustBePositive().optional().noDefault();
          }

          public static final class VectorMergePolicyConfigCliOptionRegistry
              extends CliOptionRegistry {

            @CommandLine.Option(
                names = "--indexingLuceneMergeBudgetMb",
                description =
                    "Memory budget available to background merges of vector index segments.")
            public Optional<Integer> mergeBudgetMb = Optional.empty();

            public VectorMergePolicyConfigCliOptionRegistry() {
              addIntegerLeafValue(
                  VectorMergePolicyConfig.Fields.MERGE_BUDGET_MB, () -> this.mergeBudgetMb);
            }
          }

          public static VectorMergePolicyConfig fromBson(DocumentParser parser)
              throws BsonParseException {
            return new VectorMergePolicyConfig(parser.getField(Fields.MERGE_BUDGET_MB).unwrap());
          }

          @Override
          public BsonDocument toBson() {
            return BsonDocumentBuilder.builder()
                .field(Fields.MERGE_BUDGET_MB, this.mergeBudgetMb)
                .build();
          }
        }
      }
    }

    public record MergeSchedulerConfig(
        Optional<ConcurrentMergeSchedulerConfig> concurrentSchedulerConfig)
        implements DocumentEncodable {

      static class Fields {
        public static final Field.Optional<ConcurrentMergeSchedulerConfig>
            CONCURRENT_MERGE_SCHEDULER_CONFIG =
                Field.builder("concurrent")
                    .classField(ConcurrentMergeSchedulerConfig::fromBson)
                    .disallowUnknownFields()
                    .optional()
                    .noDefault();
      }

      public static final class MergeSchedulerConfigCliOptionRegistry extends CliOptionRegistry {
        @CommandLine.Mixin
        public ConcurrentMergeSchedulerConfigCliOptionRegistry
            concurrentMergeSchedulerConfigCliOptionRegistry =
                new ConcurrentMergeSchedulerConfigCliOptionRegistry();

        public MergeSchedulerConfigCliOptionRegistry() {
          addChildRegistry(
              Fields.CONCURRENT_MERGE_SCHEDULER_CONFIG,
              this.concurrentMergeSchedulerConfigCliOptionRegistry);
        }
      }

      public static final class ConcurrentMergeSchedulerConfigCliOptionRegistry
          extends CliOptionRegistry {
        @CommandLine.Option(
            names = "--indexingLuceneMergeMaxThreadCount",
            description =
                "Max number of threads that can perform index segment merges concurrently.")
        public Optional<Integer> maxThreadCount = Optional.empty();

        public ConcurrentMergeSchedulerConfigCliOptionRegistry() {
          addIntegerLeafValue(
              ConcurrentMergeSchedulerConfig.Fields.MAX_THREAD_COUNT, () -> this.maxThreadCount);
        }
      }

      public static MergeSchedulerConfig fromBson(DocumentParser parser) throws BsonParseException {
        return new MergeSchedulerConfig(
            parser.getField(Fields.CONCURRENT_MERGE_SCHEDULER_CONFIG).unwrap());
      }

      @Override
      public BsonDocument toBson() {
        return BsonDocumentBuilder.builder()
            .field(Fields.CONCURRENT_MERGE_SCHEDULER_CONFIG, this.concurrentSchedulerConfig)
            .build();
      }

      public record ConcurrentMergeSchedulerConfig(Optional<Integer> maxThreadCount)
          implements DocumentEncodable {

        static class Fields {
          public static final Field.Optional<Integer> MAX_THREAD_COUNT =
              Field.builder("maxThreadCount").intField().mustBePositive().optional().noDefault();
        }

        public static ConcurrentMergeSchedulerConfig fromBson(DocumentParser parser)
            throws BsonParseException {
          return new ConcurrentMergeSchedulerConfig(
              parser.getField(Fields.MAX_THREAD_COUNT).unwrap());
        }

        @Override
        public BsonDocument toBson() {
          return BsonDocumentBuilder.builder()
              .field(Fields.MAX_THREAD_COUNT, this.maxThreadCount)
              .build();
        }
      }
    }
  }

  public record IndexDefinitionConfig(Optional<Integer> maxEmbeddedDocumentsNestingLevel)
      implements DocumentEncodable {

    static class Fields {
      public static final Field.Optional<Integer> MAX_EMBEDDED_DOCUMENTS_NESTING_LEVEL =
          Field.builder("maxEmbeddedDocumentsNestingLevel")
              .intField()
              .mustBeNonNegative()
              .optional()
              .noDefault();
    }

    public static IndexDefinitionConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new IndexDefinitionConfig(
          parser.getField(Fields.MAX_EMBEDDED_DOCUMENTS_NESTING_LEVEL).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.MAX_EMBEDDED_DOCUMENTS_NESTING_LEVEL, this.maxEmbeddedDocumentsNestingLevel)
          .build();
    }
  }
}
