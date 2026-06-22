package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

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

        public record VectorMergePolicyConfig(Optional<Integer> mergeBudgetMb)
            implements DocumentEncodable {

          static class Fields {
            public static final Field.Optional<Integer> MERGE_BUDGET_MB =
                Field.builder("mergeBudgetMb").intField().mustBePositive().optional().noDefault();
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
