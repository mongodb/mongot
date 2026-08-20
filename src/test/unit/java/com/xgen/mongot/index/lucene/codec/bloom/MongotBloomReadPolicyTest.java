package com.xgen.mongot.index.lucene.codec.bloom;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.Test;

public class MongotBloomReadPolicyTest {

  @Test
  public void setLoadBloomOnRead_registersUnderlyingStorageDirectory() throws IOException {
    try (Directory base = new ByteBuffersDirectory();
        Directory wrapped = new FilterDirectory(base) {}) {
      MongotBloomReadPolicy.setLoadBloomOnRead(wrapped, true);
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(base)).isTrue();
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(wrapped)).isTrue();

      MongotBloomReadPolicy.setLoadBloomOnRead(base, false);
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(base)).isFalse();
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(wrapped)).isFalse();
    }
  }

  @Test
  public void setLoadBloomOnRead_unwrapsNestedFilterDirectories() throws IOException {
    try (Directory base = new ByteBuffersDirectory();
        Directory outer = new FilterDirectory(new FilterDirectory(base) {}) {}) {
      MongotBloomReadPolicy.setLoadBloomOnRead(outer, true);
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(base)).isTrue();

      MongotBloomReadPolicy.setLoadBloomOnRead(outer, false);
      assertThat(MongotBloomReadPolicy.shouldLoadBloomOnRead(base)).isFalse();
    }
  }
}
