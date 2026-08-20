package com.xgen.mongot.index.lucene.directory;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.junit.Test;

/**
 * Covers {@link FileSystemDirectory.InstrumentedIndexInput#randomAccessSlice}.
 *
 * <p>{@link IndexInput}'s inherited fallback implements positional reads as seek plus a relative
 * read on one mutable slice, which is not safe to share between threads. The {@code _id} bloom
 * filter shares a single slice across every query thread, so the instrumented input must delegate
 * to the wrapped input's own positional implementation instead of inheriting that fallback.
 */
public class InstrumentedRandomAccessInputTest {

  private static final Path PATH = Path.of(System.getenv("TEST_TMPDIR"));

  private static final String FILE = "instrumented-random-access.blm";

  private static final int NUM_LONGS = 4_096;

  @Test
  public void randomAccessSlice_underInstrumentation_readsPositionally() throws IOException {
    try (FileSystemDirectory dir = instrumentedDirectory()) {
      writeLongs(dir);

      try (IndexInput in = dir.openInput(FILE, IOContext.DEFAULT)) {
        RandomAccessInput slice = in.randomAccessSlice(0, (long) NUM_LONGS * Long.BYTES);

        // Reading out of order must not depend on a file pointer left behind by a prior read.
        assertThat(slice.readLong((long) 7 * Long.BYTES)).isEqualTo(value(7));
        assertThat(slice.readLong((long) 4_000 * Long.BYTES)).isEqualTo(value(4_000));
        assertThat(slice.readLong((long) 7 * Long.BYTES)).isEqualTo(value(7));
      }
    }
  }

  /**
   * The regression this guards: with the inherited fallback, threads racing on the shared slice's
   * file pointer read each other's words. For a bloom bitset that surfaces as a false negative,
   * which silently suppresses a matching document.
   */
  @Test
  public void randomAccessSlice_concurrentReads_returnTheirOwnWords() throws Exception {
    try (FileSystemDirectory dir = instrumentedDirectory()) {
      writeLongs(dir);

      try (IndexInput in = dir.openInput(FILE, IOContext.DEFAULT)) {
        RandomAccessInput slice = in.randomAccessSlice(0, (long) NUM_LONGS * Long.BYTES);

        int threads = 8;
        int readsPerThread = 5_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
          List<Callable<Boolean>> tasks =
              IntStream.range(0, threads)
                  .mapToObj(
                      t ->
                          (Callable<Boolean>)
                              () -> {
                                for (int i = 0; i < readsPerThread; i++) {
                                  int index = (t * 13 + i * 7) % NUM_LONGS;
                                  if (slice.readLong((long) index * Long.BYTES) != value(index)) {
                                    return false;
                                  }
                                }
                                return true;
                              })
                  .collect(Collectors.toList());

          List<Future<Boolean>> results = pool.invokeAll(tasks);
          for (Future<Boolean> result : results) {
            assertThat(result.get(60, TimeUnit.SECONDS)).isTrue();
          }
        } finally {
          pool.shutdownNow();
        }
      }
    }
  }

  @Test
  public void randomAccessSlice_underInstrumentation_countsBytesRead() throws IOException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ByteReadCollector collector = new ByteReadCollector(new MetricsFactory("test", registry));

    try (FileSystemDirectory dir = new FileSystemDirectory(PATH, Optional.of(collector))) {
      writeLongs(dir);

      try (IndexInput in = dir.openInput(FILE, IOContext.DEFAULT)) {
        RandomAccessInput slice = in.randomAccessSlice(0, (long) NUM_LONGS * Long.BYTES);
        for (int i = 0; i < 10; i++) {
          slice.readLong((long) i * Long.BYTES);
        }
      }

      // Instrumentation must survive the delegation: positional reads are still counted, and
      // against the .blm extension.
      assertThat(bytesRead(registry)).isAtLeast(10L * Long.BYTES);
    }
  }

  private static FileSystemDirectory instrumentedDirectory() throws IOException {
    ByteReadCollector collector =
        new ByteReadCollector(new MetricsFactory("test", new SimpleMeterRegistry()));
    return new FileSystemDirectory(PATH, Optional.of(collector));
  }

  private static void writeLongs(FileSystemDirectory dir) throws IOException {
    try {
      dir.deleteFile(FILE);
    } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException ignored) {
      // First write in this directory.
    }
    try (IndexOutput out = dir.createOutput(FILE, IOContext.DEFAULT)) {
      for (int i = 0; i < NUM_LONGS; i++) {
        out.writeLong(value(i));
      }
    }
  }

  private static long value(int index) {
    // Distinct, position-dependent values so a torn read cannot coincidentally look correct.
    return 0x5DEECE66DL * (index + 1) + 0xB;
  }

  private static long bytesRead(SimpleMeterRegistry registry) {
    // MetricsFactory namespaces the counter, so match on the suffix rather than the bare name.
    return registry.getMeters().stream()
        .filter(meter -> meter.getId().getName().endsWith(ByteReadCollector.METRIC_NAME))
        .filter(meter -> "blm".equals(meter.getId().getTag("fileType")))
        .mapToLong(meter -> (long) ((Counter) meter).count())
        .sum();
  }
}
