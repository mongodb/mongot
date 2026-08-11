package com.xgen.mongot.util.mongodb;

import org.bson.BsonTimestamp;

/**
 * Supplies the sync source's current majority-committed optime.
 *
 * <p>Distinct from {@code
 * com.xgen.mongot.replication.mongodb.initialsync.ServerClusterTimeProvider} in that it carries no
 * initial sync retry semantics: callers outside replication only need the timestamp or a plain
 * failure.
 */
@FunctionalInterface
public interface ReadConcernMajorityOpTimeProvider {
  BsonTimestamp getReadConcernMajorityOpTime() throws CheckedMongoException;
}
