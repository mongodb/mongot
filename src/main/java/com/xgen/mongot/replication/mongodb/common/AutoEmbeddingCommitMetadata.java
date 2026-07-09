package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

/**
 * Auto-embedding-only state persisted inside the derived index's {@link IndexCommitUserData} so the
 * index can be reported on a restart that cannot reach mongod.
 *
 * <p>{@code wasMatViewReady} snapshots whether the materialized view data was ready at commit time.
 * It is not a persisted "composite index was queryable" bit; it is the fallback gate used before
 * making the derived Lucene index queryable when mongod is unreachable and the lease cannot be
 * read.
 *
 * <p>Parsed with {@code allowUnknownFields} (see {@link IndexCommitUserData}) so that fields added
 * here in the future stay readable by binaries that predate them.
 */
public record AutoEmbeddingCommitMetadata(boolean wasMatViewReady) implements DocumentEncodable {

  private static class Fields {
    // On-disk fallback for whether MV data was ready before making Lucene queryable.
    private static final Field.Required<Boolean> WAS_MAT_VIEW_READY =
        Field.builder("wasMatViewReady").booleanField().required();
  }

  public static AutoEmbeddingCommitMetadata fromBson(DocumentParser parser)
      throws BsonParseException {
    return new AutoEmbeddingCommitMetadata(parser.getField(Fields.WAS_MAT_VIEW_READY).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.WAS_MAT_VIEW_READY, this.wasMatViewReady)
        .build();
  }
}
