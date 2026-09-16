package com.xgen.mongot.monitor.index;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import java.io.IOException;
import java.util.OptionalLong;
import org.bson.types.ObjectId;
import org.junit.Test;

public class OnDiskIndexSizeSamplerTest {

  private static InitializedSearchIndex mockIndex(GenerationId generationId, long sizeBytes)
      throws IOException {
    InitializedSearchIndex index = mock(InitializedSearchIndex.class);
    when(index.getGenerationId()).thenReturn(generationId);
    when(index.refreshAndGetIndexSize()).thenReturn(sizeBytes);
    return index;
  }

  private static GenerationId newId() {
    return new GenerationId(new ObjectId(), Generation.CURRENT);
  }

  @Test
  public void sampleTotalIndexBytes_sumsInitializedIndexes() throws Exception {
    var initialized = new InitializedIndexCatalog();
    initialized.addIndex(mockIndex(newId(), 100L));
    initialized.addIndex(mockIndex(newId(), 50L));

    OptionalLong total = new OnDiskIndexSizeSampler(initialized).sampleTotalIndexBytes();

    assertThat(total).isEqualTo(OptionalLong.of(150L));
  }

  @Test
  public void sampleTotalIndexBytes_generationsInEveryState_countsAllOnDiskBytes()
      throws Exception {
    ObjectId rebuiltIndexId = new ObjectId();
    var initialized = new InitializedIndexCatalog();
    initialized.addIndex(mockIndex(new GenerationId(rebuiltIndexId, Generation.FIRST), 100L));
    initialized.addIndex(mockIndex(new GenerationId(rebuiltIndexId, Generation.CURRENT), 60L));
    initialized.addIndex(mockIndex(newId(), 40L));

    OptionalLong total = new OnDiskIndexSizeSampler(initialized).sampleTotalIndexBytes();

    assertThat(total).isEqualTo(OptionalLong.of(200L));
  }

  @Test
  public void sampleTotalIndexBytes_noInitializedIndexes_isZero() throws Exception {
    OptionalLong total =
        new OnDiskIndexSizeSampler(new InitializedIndexCatalog()).sampleTotalIndexBytes();

    assertThat(total).isEqualTo(OptionalLong.of(0L));
  }

  @Test
  public void sampleTotalIndexBytes_refreshFailure_returnsEmpty() throws Exception {
    InitializedSearchIndex failingIndex = mock(InitializedSearchIndex.class);
    when(failingIndex.getGenerationId()).thenReturn(newId());
    when(failingIndex.refreshAndGetIndexSize()).thenThrow(new IllegalStateException("closed"));
    var initialized = new InitializedIndexCatalog();
    initialized.addIndex(mockIndex(newId(), 100L));
    initialized.addIndex(failingIndex);

    OptionalLong total = new OnDiskIndexSizeSampler(initialized).sampleTotalIndexBytes();

    assertThat(total).isEqualTo(OptionalLong.empty());
  }

  @Test
  public void sampleTotalIndexBytes_ioException_propagates() throws Exception {
    InitializedSearchIndex index = mock(InitializedSearchIndex.class);
    when(index.getGenerationId()).thenReturn(newId());
    when(index.refreshAndGetIndexSize()).thenThrow(new IOException("walk failed"));
    var initialized = new InitializedIndexCatalog();
    initialized.addIndex(index);

    assertThrows(
        IOException.class, () -> new OnDiskIndexSizeSampler(initialized).sampleTotalIndexBytes());
  }
}
