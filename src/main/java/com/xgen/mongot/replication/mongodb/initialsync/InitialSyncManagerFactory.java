package com.xgen.mongot.replication.mongodb.initialsync;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import java.util.Optional;

interface InitialSyncManagerFactory {

  /**
   * Builds the manager that runs one index's initial sync.
   *
   * @param collectionInfo the collection namespace resolution returned, carried so that partition
   *     planning can read its creation options without resolving the namespace again
   */
  InitialSyncManager create(
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      MongoNamespace namespace,
      MongoDbCollectionInfo collectionInfo,
      Optional<InitialSyncResumeInfo> resumeInfo);
}
