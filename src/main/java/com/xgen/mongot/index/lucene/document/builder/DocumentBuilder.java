package com.xgen.mongot.index.lucene.document.builder;

import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import java.io.IOException;
import org.apache.lucene.document.Document;

/** Is able to create a {@code Document} that can be inserted into a Lucene index. */
public interface DocumentBuilder extends DocumentHandler {
  Document build() throws IOException;
}
