package com.xgen.mongot.config.util;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import java.util.UUID;
import org.bson.types.ObjectId;

/**
 * The minimal view of an index generation that identifies it: enough to place it in the config
 * journal, sort it, and check uniqueness invariants against it, but not enough to build an index
 * from.
 *
 * <p>This exists so that config journal entries whose definition could not be parsed (and which are
 * therefore quarantined rather than fatal) can still participate in the checks that only need an
 * identity. Fully parsed definitions and thinly parsed quarantined entries both expose it.
 */
public interface IndexIdentity {

  ObjectId indexId();

  Generation generation();

  String database();

  UUID collectionUuid();

  String name();

  default GenerationId generationId() {
    return generation().generationId(indexId());
  }

  /**
   * The key under which an index must be unique: an index name is unique within a namespace, not
   * globally.
   */
  default String namespaceKey() {
    return namespaceKey(database(), collectionUuid(), name());
  }

  /** See {@link #namespaceKey()}. */
  static String namespaceKey(String database, UUID collectionUuid, String name) {
    return String.format("(%s %s %s)", database, collectionUuid, name);
  }
}
