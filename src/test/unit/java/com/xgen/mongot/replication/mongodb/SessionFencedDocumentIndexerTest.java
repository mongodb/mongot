package com.xgen.mongot.replication.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.CommitDataStagingWriter;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

public class SessionFencedDocumentIndexerTest {

  // ── during tenure: standard indexer behaviour ────────────────────────────────

  @Test
  public void indexDocumentEvent_duringTenure_delegatesToWriter() throws Exception {
    var fixture = new Fixture();
    var indexer = fixture.newTenuredIndexer(steadyCommitData());
    var event = mock(DocumentEvent.class);

    indexer.indexDocumentEvent(event);

    verify(fixture.writer).updateIndex(event);
  }

  @Test
  public void prepareBatch_duringTenure_delegatesToWriter() throws Exception {
    var fixture = new Fixture();
    var indexer = fixture.newTenuredIndexer(steadyCommitData());
    List<DocumentEvent> input = List.of(mock(DocumentEvent.class));
    List<DocumentEvent> prepared = List.of(mock(DocumentEvent.class), mock(DocumentEvent.class));
    when(fixture.writer.prepareBatch(input)).thenReturn(prepared);

    List<DocumentEvent> result = indexer.prepareBatch(input);

    assertThat(result).isSameInstanceAs(prepared);
  }

  @Test
  public void stageCommitUserData_duringTenure_stagesTokenSeededFromPreparedBaseline()
      throws Exception {
    // The session's resume position is seeded from the token the writer was prepared with, not from
    // the shared/live commit user data (fixes N4): the delegate constructor reads EMPTY, but the
    // stage must carry the distinct prepared baseline.
    var fixture = new Fixture();
    var baseline = steadyCommitData();
    var indexer = fixture.newTenuredIndexer(baseline);

    indexer.stageCommitUserData();

    verify(fixture.stagingWriter).stageUserData(baseline.toEncodedData());
  }

  @Test
  public void commit_duringTenure_stagesThenCommitsSeededBaseline() throws Exception {
    var fixture = new Fixture();
    var baseline = steadyCommitData();
    var indexer = fixture.newTenuredIndexer(baseline);

    indexer.commit();

    verify(fixture.stagingWriter).stageUserData(baseline.toEncodedData());
    verify(fixture.writer).commit(baseline.toEncodedData());
  }

  @Test
  public void commit_afterUpdateCommitUserData_usesUpdatedToken() throws Exception {
    var fixture = new Fixture();
    var indexer = fixture.newTenuredIndexer(IndexCommitUserData.EMPTY);
    var updated = steadyCommitData();

    indexer.updateCommitUserData(updated);
    indexer.commit();

    verify(fixture.writer).commit(updated.toEncodedData());
  }

  @Test
  public void commit_afterUpdateCommitUserDataSupplier_usesSuppliedToken() throws Exception {
    // The live-supplier path (used by the parallel collection scan) is read at commit time.
    var fixture = new Fixture();
    var indexer = fixture.newTenuredIndexer(IndexCommitUserData.EMPTY);
    var supplied = steadyCommitData();

    indexer.updateCommitUserDataSupplier(() -> supplied);
    indexer.commit();

    verify(fixture.writer).commit(supplied.toEncodedData());
  }

  @Test
  public void clearIndex_duringTenure_clearsIndexAndUpdatesToken() throws Exception {
    var fixture = new Fixture();
    var indexer = fixture.newTenuredIndexer(IndexCommitUserData.EMPTY);
    var cleared = steadyCommitData();

    indexer.clearIndex(cleared);

    verify(fixture.index).clear(cleared.toEncodedData());
    // The next commit resumes from the token clearIndex installed.
    indexer.commit();
    verify(fixture.writer).commit(cleared.toEncodedData());
  }

  @Test
  public void getIndexDefinition_delegates() {
    var fixture = new Fixture();
    var indexer = fixture.newTenuredIndexer(IndexCommitUserData.EMPTY);

    assertThat(indexer.getIndexDefinition()).isSameInstanceAs(fixture.definition);
  }

  // ── tenure fencing: stale work is dropped, never rejected ──────────

  @Test
  public void batchAndStatusCalls_afterEndTenure_areDroppedAndCounted() throws Exception {
    var fixture = new Fixture();
    var indexer = fixture.newTenuredIndexer(steadyCommitData());

    indexer.endTenure();

    indexer.indexDocumentEvent(mock(DocumentEvent.class));
    indexer.stageCommitUserData();
    indexer.commit();
    indexer.clearIndex(steadyCommitData());
    indexer.updateCommitUserData(steadyCommitData());
    indexer.updateCommitUserDataSupplier(SessionFencedDocumentIndexerTest::steadyCommitData);

    // Nothing reaches the write path or the index: stale work from an ended session is dropped.
    verify(fixture.writer, never()).updateIndex(any());
    verify(fixture.stagingWriter, never()).stageUserData(any());
    verify(fixture.writer, never()).commit(any());
    verify(fixture.index, never()).clear(any());
    // One count per dropped mutating call (six above).
    assertThat(staleWrites(fixture.meterRegistry)).isEqualTo(6.0);
  }

  @Test
  public void prepareBatch_afterEndTenure_returnsInputUnmodifiedAndDoesNotDelegate()
      throws Exception {
    var fixture = new Fixture();
    var indexer = fixture.newTenuredIndexer(steadyCommitData());
    List<DocumentEvent> input = List.of(mock(DocumentEvent.class));

    indexer.endTenure();
    List<DocumentEvent> result = indexer.prepareBatch(input);

    assertThat(result).isSameInstanceAs(input);
    verify(fixture.writer, never()).prepareBatch(any());
    assertThat(staleWrites(fixture.meterRegistry)).isEqualTo(1.0);
  }

  @Test
  public void mutatingCalls_beforeBeginTenure_areDropped() throws Exception {
    // The fence is closed until beginTenure: a call before the session starts is dropped, so a RIM
    // whose init somehow reaches the indexer before RRIM begins the tenure cannot write.
    var fixture = new Fixture();
    var indexer =
        new SessionFencedDocumentIndexer(fixture.index, steadyCommitData(), fixture.meterRegistry);

    indexer.commit();

    verify(fixture.writer, never()).commit(any());
    assertThat(staleWrites(fixture.meterRegistry)).isEqualTo(1.0);
  }

  @Test
  public void endTenure_isIdempotent() throws Exception {
    var fixture = new Fixture();
    var indexer = fixture.newTenuredIndexer(steadyCommitData());

    indexer.endTenure();
    indexer.endTenure(); // second call must not throw or change behaviour
    indexer.commit();

    verify(fixture.writer, never()).commit(any());
    assertThat(staleWrites(fixture.meterRegistry)).isEqualTo(1.0);
  }

  // ── helpers ───────────────────────────────────────────────────────────────────

  private static IndexCommitUserData steadyCommitData() {
    return IndexCommitUserData.createChangeStreamResume(
        ChangeStreamResumeInfo.create(
            new MongoNamespace("database", "collection"),
            new BsonDocument("_data", new BsonString("820000000000000000"))),
        IndexFormatVersion.CURRENT.versionNumber);
  }

  private static double staleWrites(SimpleMeterRegistry registry) {
    return registry
        .find(SessionFencedDocumentIndexer.STALE_SESSION_WRITE_COUNTER_NAME)
        .counters()
        .stream()
        .mapToDouble(Counter::count)
        .sum();
  }

  /**
   * A mock VectorLite index whose writer is both an {@link IndexWriter} and a {@link
   * CommitDataStagingWriter}, mirroring {@code VectorliteCompositeIndexWriter}. The delegate {@link
   * com.xgen.mongot.replication.mongodb.common.DefaultDocumentIndexer} reads EMPTY commit user data
   * on construction, so a distinct seeded baseline proves the session owns its own resume position.
   */
  private static final class Fixture {
    final InitializedVectorIndex index = mock(InitializedVectorIndex.class);
    final IndexWriter writer =
        mock(IndexWriter.class, withSettings().extraInterfaces(CommitDataStagingWriter.class));
    final CommitDataStagingWriter stagingWriter = (CommitDataStagingWriter) this.writer;
    final VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder().withCustomEngineVectorField("vector", 3).build();
    final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    Fixture() {
      var generation = mockIndexGeneration(this.definition);
      when(this.index.getDefinition()).thenReturn(this.definition);
      when(this.index.getGenerationId()).thenReturn(generation.getGenerationId());
      when(this.index.getWriter()).thenReturn(this.writer);
      when(this.writer.getCommitUserData()).thenReturn(IndexCommitUserData.EMPTY.toEncodedData());
    }

    SessionFencedDocumentIndexer newTenuredIndexer(IndexCommitUserData baseline) {
      var indexer = new SessionFencedDocumentIndexer(this.index, baseline, this.meterRegistry);
      indexer.beginTenure(1L);
      return indexer;
    }
  }
}
