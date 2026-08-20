package com.xgen.mongot.index.lucene.codec.bloom;

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

/**
 * Per-index override for whether bloom postings are wired up at all when a {@link
 * org.apache.lucene.index.DirectoryReader} opens bloom-encoded segments. When disabled, the
 * producer returns the raw delegate and the {@code .blm} is never opened.
 *
 * <p>Defaults to {@code false} when no entry exists for a {@link Directory}. Production callers use
 * {@link #setLoadBloomOnRead(Directory, boolean)} before opening or merging segments. Read policy
 * is resolved internally from {@link org.apache.lucene.index.SegmentInfo#dir} (not {@link
 * org.apache.lucene.index.SegmentReadState#directory}, which may be a compound-file reader).
 *
 * <p>Value is set per index partition directory by index lifecycle code (initial sync vs steady
 * state) so steady-state indices can skip bloom entirely while retaining correct {@code _id}
 * lookups via delegate postings. Note this is orthogonal to <em>where</em> the bitsets live: when
 * bloom is enabled the bits are always read from the memory-mapped {@code .blm}, never the heap.
 */
public final class MongotBloomReadPolicy {

  private static final Set<Directory> LOAD_BLOOM_DIRECTORIES = Sets.newConcurrentHashSet();

  private MongotBloomReadPolicy() {}

  /**
   * Sets whether bloom bitsets for {@code directory} should be loaded on segment open. Registers
   * the underlying storage {@link Directory} after unwrapping any {@link FilterDirectory} chain.
   */
  public static void setLoadBloomOnRead(Directory directory, boolean load) {
    Directory storageDirectory = unwrapFilterDirectory(directory);
    if (load) {
      LOAD_BLOOM_DIRECTORIES.add(storageDirectory);
    } else {
      LOAD_BLOOM_DIRECTORIES.remove(storageDirectory);
    }
  }

  /**
   * Resolves policy for the index that owns {@code directory}. Returns the default when the
   * directory is not registered. Unwraps {@link FilterDirectory} wrappers so registrations on
   * underlying index directories are found.
   */
  public static boolean shouldLoadBloomOnRead(Directory directory) {
    return LOAD_BLOOM_DIRECTORIES.contains(unwrapFilterDirectory(directory));
  }

  /** Returns the delegate at the end of a {@link FilterDirectory} chain. */
  static Directory unwrapFilterDirectory(Directory directory) {
    if (directory instanceof FilterDirectory filterDirectory) {
      return unwrapFilterDirectory(filterDirectory.getDelegate());
    }
    return directory;
  }
}
