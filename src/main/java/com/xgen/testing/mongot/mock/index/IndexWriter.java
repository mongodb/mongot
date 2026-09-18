package com.xgen.testing.mongot.mock.index;

import static org.mockito.Mockito.mock;

import com.xgen.mongot.index.CommitDataStagingWriter;
import com.xgen.mongot.index.EncodedUserData;
import java.util.Optional;
import org.mockito.Mockito;

public class IndexWriter {
  public static com.xgen.mongot.index.IndexWriter mockIndexWriter() {
    com.xgen.mongot.index.IndexWriter writer = mock(com.xgen.mongot.index.IndexWriter.class);
    Mockito.lenient().when(writer.getCommitUserData()).thenReturn(EncodedUserData.EMPTY);
    Mockito.lenient().when(writer.exceededLimits()).thenReturn(Optional.empty());
    return writer;
  }

  /** A writer that also implements {@link CommitDataStagingWriter}, like the vectorlite writer. */
  public static com.xgen.mongot.index.IndexWriter mockStagingIndexWriter() {
    com.xgen.mongot.index.IndexWriter writer =
        mock(
            com.xgen.mongot.index.IndexWriter.class,
            Mockito.withSettings().extraInterfaces(CommitDataStagingWriter.class));
    Mockito.lenient().when(writer.getCommitUserData()).thenReturn(EncodedUserData.EMPTY);
    Mockito.lenient().when(writer.exceededLimits()).thenReturn(Optional.empty());
    return writer;
  }
}
