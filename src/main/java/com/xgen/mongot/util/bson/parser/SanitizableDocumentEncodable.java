package com.xgen.mongot.util.bson.parser;

import org.bson.BsonDocument;

/** An interface used to provide a sanitized version of a BSON document. */
public interface SanitizableDocumentEncodable extends DocumentEncodable {
  BsonDocument toSanitizedBson();
}
