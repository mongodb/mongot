package com.xgen.mongot.index.lucene.directory;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.junit.Assert;
import org.junit.Test;

public class IndexDirectoryFactoryTest {

  private static final Path ROOT_PATH = Path.of(System.getenv("TEST_TMPDIR"));
  private static final Path INDEX_PATH = Path.of(System.getenv("TEST_TMPDIR"), "index");
  private static final Path METADATA_PATH = Path.of(System.getenv("TEST_TMPDIR"), "indexMapping");

  @Test
  public void testWithoutNrtCache() throws IOException {
    LuceneConfig config =
        LuceneConfigBuilder.builder().dataPath(ROOT_PATH).nrtCacheEnabled(false).build();
    IndexDirectoryFactory directoryFactory =
        new IndexDirectoryFactory(INDEX_PATH, METADATA_PATH, config, 1, Optional.empty());
    Directory directory = directoryFactory.create(0);
    assertThat(directory).isInstanceOf(FileSystemDirectory.class);
  }

  @Test
  public void testWithNrtCache() throws IOException {
    LuceneConfig config =
        LuceneConfigBuilder.builder().dataPath(ROOT_PATH).nrtCacheEnabled(true).build();
    IndexDirectoryFactory directoryFactory =
        new IndexDirectoryFactory(INDEX_PATH, METADATA_PATH, config, 1, Optional.empty());
    Directory directory = directoryFactory.create(0);
    assertThat(directory).isInstanceOf(NRTCachingDirectory.class);
  }

  @Test
  public void testGetIndexPartitionPath() {
    record TestCase(int partitionId, int numPartitions, Path expectedPath) {}

    var testCases =
        List.of(
            new TestCase(0, 1, INDEX_PATH),
            new TestCase(0, 2, INDEX_PATH.resolve("0")),
            new TestCase(58, 64, INDEX_PATH.resolve("3a")));

    LuceneConfig config =
        LuceneConfigBuilder.builder().dataPath(ROOT_PATH).nrtCacheEnabled(true).build();
    for (var testCase : testCases) {
      var dirFactory =
          new IndexDirectoryFactory(
              INDEX_PATH, METADATA_PATH, config, testCase.numPartitions, Optional.empty());
      Assert.assertEquals(
          testCase.expectedPath, dirFactory.getIndexPartitionDataPath(testCase.partitionId));
    }
  }

  @Test
  public void create_withPrewarmEnabled_recordsCacheWarmerWindow() throws IOException {
    LuceneConfig config =
        LuceneConfigBuilder.builder().dataPath(ROOT_PATH).nrtCacheEnabled(false).build();
    IndexDirectoryFactory directoryFactory =
        spy(
            new IndexDirectoryFactory(
                INDEX_PATH,
                METADATA_PATH,
                config,
                1,
                Optional.empty(),
                true,
                Optional.of(new CacheWarmerTimer())));

    Directory directory = directoryFactory.create(0);

    assertThat(directory).isInstanceOf(FileSystemDirectory.class);
    verify(directoryFactory).recordCacheWarmerWindow(anyLong(), anyLong());
  }

  @Test
  public void create_withPrewarmDisabled_doesNotRecordCacheWarmerWindow() throws IOException {
    LuceneConfig config =
        LuceneConfigBuilder.builder().dataPath(ROOT_PATH).nrtCacheEnabled(false).build();
    IndexDirectoryFactory directoryFactory =
        spy(
            new IndexDirectoryFactory(
                INDEX_PATH,
                METADATA_PATH,
                config,
                1,
                Optional.empty(),
                false,
                Optional.of(new CacheWarmerTimer())));

    Directory directory = directoryFactory.create(0);

    assertThat(directory).isInstanceOf(FileSystemDirectory.class);
    verify(directoryFactory, never()).recordCacheWarmerWindow(anyLong(), anyLong());
  }
}
