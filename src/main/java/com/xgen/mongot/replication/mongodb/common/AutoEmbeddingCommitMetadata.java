package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Auto-embedding-only state persisted inside the derived index's {@link IndexCommitUserData} so the
 * index can be reported and served on a restart that cannot reach mongod.
 *
 * <p>{@code wasMatViewReady} snapshots whether the materialized view data was ready at commit time.
 * It is not a persisted "composite index was queryable" bit; it is the fallback gate used before
 * making the derived Lucene index queryable when mongod is unreachable and the lease cannot be
 * read.
 *
 * <p>{@code materializedViewDatabaseName} and {@code materializedViewCollectionMetadata} carry the
 * inputs needed to rebuild the derived index definition without mongod: the resolved
 * (internal-search) database name, the mongod-assigned collection UUID, and the MV-sourced schema
 * metadata. The database name is persisted rather than recomputed because on multi-tenant clusters
 * it is namespaced per tenant (e.g. {@code "acme___mdb_internal_search"}); recomputing it offline
 * would re-depend on the tenant-resolution heuristic staying stable across the binaries that write
 * and read the commit. Both are optional: present only once a binary that persists them has
 * committed, so a derived Lucene last committed by an older binary (which wrote {@code
 * wasMatViewReady} alone) still parses.
 *
 * <p>Parsed with {@code allowUnknownFields} (see {@link IndexCommitUserData}) so that fields added
 * here in the future stay readable by binaries that predate them.
 */
public record AutoEmbeddingCommitMetadata(
    boolean wasMatViewReady,
    Optional<String> materializedViewDatabaseName,
    Optional<MaterializedViewCollectionMetadata> materializedViewCollectionMetadata)
    implements DocumentEncodable {

  public AutoEmbeddingCommitMetadata {
    // The optionals are part of the persisted invariant; reject null so a bad caller fails here
    // rather than NPE-ing later in toBson() or a downstream read.
    Objects.requireNonNull(materializedViewDatabaseName, "materializedViewDatabaseName");
    Objects.requireNonNull(
        materializedViewCollectionMetadata, "materializedViewCollectionMetadata");
  }

  /** Convenience for callers that only set {@code wasMatViewReady} (no reconstruction metadata). */
  public AutoEmbeddingCommitMetadata(boolean wasMatViewReady) {
    this(wasMatViewReady, Optional.empty(), Optional.empty());
  }

  private static class Fields {
    // On-disk fallback for whether MV data was ready before making Lucene queryable.
    private static final Field.Required<Boolean> WAS_MAT_VIEW_READY =
        Field.builder("wasMatViewReady").booleanField().required();

    // Resolved (tenant-namespaced) internal-search database of the materialized view.
    private static final Field.Optional<String> MATERIALIZED_VIEW_DATABASE_NAME =
        Field.builder("materializedViewDatabaseName").stringField().optional().noDefault();

    private static final Field.Optional<MaterializedViewCollectionMetadata>
        MATERIALIZED_VIEW_COLLECTION_METADATA =
            Field.builder("materializedViewCollectionMetadata")
                .classField(MaterializedViewCollectionMetadata::fromBson)
                .allowUnknownFields()
                .optional()
                .noDefault();
  }

  public static AutoEmbeddingCommitMetadata fromBson(DocumentParser parser)
      throws BsonParseException {
    return new AutoEmbeddingCommitMetadata(
        parser.getField(Fields.WAS_MAT_VIEW_READY).unwrap(),
        parser.getField(Fields.MATERIALIZED_VIEW_DATABASE_NAME).unwrap(),
        parser.getField(Fields.MATERIALIZED_VIEW_COLLECTION_METADATA).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.WAS_MAT_VIEW_READY, this.wasMatViewReady)
        .field(Fields.MATERIALIZED_VIEW_DATABASE_NAME, this.materializedViewDatabaseName)
        .field(
            Fields.MATERIALIZED_VIEW_COLLECTION_METADATA, this.materializedViewCollectionMetadata)
        .build();
  }
}
