package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.index.DocumentEvent;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.junit.Test;

public class IdTypeObservingDocumentIndexerTest {

  @Test
  public void testCallbackFiredOnFirstDocument() throws Exception {
    DocumentIndexer delegate = mock(DocumentIndexer.class);
    List<String> observed = new ArrayList<>();
    IdTypeObservingDocumentIndexer indexer =
        IdTypeObservingDocumentIndexer.wrap(delegate, observed::add);

    DocumentEvent event = DocumentEvent.createDelete(new BsonObjectId());
    indexer.indexDocumentEvent(event);

    assertThat(observed).containsExactly("OBJECT_ID");
    verify(delegate).indexDocumentEvent(event);
  }

  @Test
  public void testCallbackFiredOnlyOnce() throws Exception {
    DocumentIndexer delegate = mock(DocumentIndexer.class);
    List<String> observed = new ArrayList<>();
    IdTypeObservingDocumentIndexer indexer =
        IdTypeObservingDocumentIndexer.wrap(delegate, observed::add);

    DocumentEvent first = DocumentEvent.createDelete(new BsonObjectId());
    DocumentEvent second = DocumentEvent.createDelete(new BsonString("some-id"));
    indexer.indexDocumentEvent(first);
    indexer.indexDocumentEvent(second);

    assertThat(observed).hasSize(1);
    assertThat(observed).containsExactly("OBJECT_ID");
    verify(delegate, times(2)).indexDocumentEvent(any());
  }

  @Test
  public void testDelegateStillCalledWhenCallbackThrows() throws Exception {
    DocumentIndexer delegate = mock(DocumentIndexer.class);
    IdTypeObservingDocumentIndexer indexer =
        IdTypeObservingDocumentIndexer.wrap(
            delegate,
            ignored -> {
              throw new RuntimeException("callback error");
            });

    DocumentEvent event = DocumentEvent.createDelete(new BsonObjectId());
    indexer.indexDocumentEvent(event);

    verify(delegate).indexDocumentEvent(event);
  }
}
