package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import org.bson.types.ObjectId;

/** Treats an index as high priority if its previous batch delivered any events. */
final class HadDocumentsPriorityStrategy implements ChangeStreamPriorityStrategy {

  @Override
  public boolean isHighPriority(ObjectId indexId, ChangeStreamBatch batch) {
    return !batch.getRawEvents().isEmpty();
  }
}
