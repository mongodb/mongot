/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xgen.mongot.index.lucene.vector;

import com.google.auto.service.AutoService;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * A drop-in replacement for {@code Lucene99HnswVectorsFormat} that substitutes {@link
 * Lucene99NativeMemorySegmentVectorScorer} as the flat vector scorer, enabling native SIMD
 * acceleration for byte vector comparisons when the underlying {@link
 * org.apache.lucene.store.IndexInput} is memory-segment backed.
 *
 * <p>The codec name is kept as {@code "Lucene99HnswVectorsFormat"} so that segments written by this
 * format are read back transparently by Lucene's standard reader chain.
 */
@AutoService(KnnVectorsFormat.class)
public final class Lucene99NativeHnswVectorsFormat extends KnnVectorsFormat {

  static final String META_CODEC_NAME = "Lucene99HnswVectorsFormatMeta";
  static final String VECTOR_INDEX_CODEC_NAME = "Lucene99HnswVectorsFormatIndex";
  static final String META_EXTENSION = "vem";
  static final String VECTOR_INDEX_EXTENSION = "vex";

  /**
   * The {@code tinySegmentsThreshold} every mongot HNSW format passes to {@link
   * Lucene99HnswVectorsWriter}: 0 means an HNSW graph is always built, matching what mongot wrote
   * on Lucene 9.11.1 and 10.1.0. Lucene 10.4's upstream default ({@code
   * Lucene99HnswVectorsFormat#HNSW_GRAPH_THRESHOLD} = 100) skips the graph for segments with fewer
   * vectors, writing {@code numLevels=0} metadata that 9.11.1/10.1.0 readers NPE on at query time —
   * breaking rollback. Defined once here (mirroring how Lucene shares the upstream constant) and
   * imported by every other mongot HNSW format.
   */
  public static final int DEFAULT_HNSW_GRAPH_THRESHOLD = 0;

  /**
   * A maximum configurable maximum max conn.
   *
   * <p>NOTE: We eagerly populate {@code float[MAX_CONN*2]} and {@code int[MAX_CONN*2]}, so
   * exceptionally large numbers here will use an inordinate amount of heap.
   */
  public static final int MAXIMUM_MAX_CONN = 512;

  /** Default number of maximum connections per node */
  public static final int DEFAULT_MAX_CONN = HnswGraphBuilder.DEFAULT_MAX_CONN;

  /**
   * The maximum size of the queue to maintain while searching during graph construction. This
   * maximum value preserves the ratio of the {@code DEFAULT_BEAM_WIDTH}/{@code DEFAULT_MAX_CONN}
   * (i.e. {@code 6.25 * 16 = 3200}).
   */
  public static final int MAXIMUM_BEAM_WIDTH = 3200;

  /**
   * Default number of the size of the queue maintained while searching during graph construction.
   */
  public static final int DEFAULT_BEAM_WIDTH = HnswGraphBuilder.DEFAULT_BEAM_WIDTH;

  /** Default to use single thread merge */
  public static final int DEFAULT_NUM_MERGE_WORKER = 1;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  /**
   * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
   * {@link Lucene99NativeHnswVectorsFormat#DEFAULT_MAX_CONN}. See {@link HnswGraph} for more
   * details.
   */
  private final int maxConn;

  /**
   * The number of candidate neighbors to track while searching the graph for each newly inserted
   * node. Defaults to {@link Lucene99NativeHnswVectorsFormat#DEFAULT_BEAM_WIDTH}. See {@link
   * HnswGraph} for details.
   */
  private final int beamWidth;

  /**
   * The format for storing, reading, and merging vectors on disk. Uses {@link
   * Lucene99NativeMemorySegmentVectorScorer} to accelerate byte vector comparisons via native SIMD,
   * falling back to Lucene's default scorer for float vectors or non-mmapped inputs.
   */
  private static final FlatVectorsFormat flatVectorsFormat =
      new Lucene99FlatVectorsFormat(
          new Lucene99NativeMemorySegmentVectorScorer(
              FlatVectorScorerUtil.getLucene99FlatVectorsScorer()));

  /** Number of merge worker threads; values above 1 require {@link #mergeExec}. */
  private final int numMergeWorkers;

  /** Executor used by the vector writers this format creates to parallelize merges. */
  private @Nullable final TaskExecutor mergeExec;

  /** Tiny-segment graph threshold and graph write version; see {@link HnswWriteParams}. */
  private final HnswWriteParams hnswWriteParams;

  /** Constructs a format using default graph construction parameters */
  public Lucene99NativeHnswVectorsFormat() {
    this(
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NUM_MERGE_WORKER,
        null,
        new HnswWriteParams(DEFAULT_HNSW_GRAPH_THRESHOLD, Lucene99HnswVectorsFormat.VERSION_START));
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param numMergeWorkers number of workers (threads) that will be used when doing merge. If
   *     larger than 1, a non-null {@link ExecutorService} must be passed as mergeExec
   * @param mergeExec the {@link ExecutorService} that will be used by ALL vector writers that are
   *     generated by this format to do the merge. If null, the configured {@link
   *     MergeScheduler#getIntraMergeExecutor(MergePolicy.OneMerge)} is used.
   * @param hnswWriteParams the tiny-segment graph threshold and graph write version
   */
  public Lucene99NativeHnswVectorsFormat(
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      @Nullable ExecutorService mergeExec,
      HnswWriteParams hnswWriteParams) {
    super("Lucene99HnswVectorsFormat");
    if (maxConn <= 0 || maxConn > MAXIMUM_MAX_CONN) {
      throw new IllegalArgumentException(
          "maxConn must be positive and less than or equal to "
              + MAXIMUM_MAX_CONN
              + "; maxConn="
              + maxConn);
    }
    if (beamWidth <= 0 || beamWidth > MAXIMUM_BEAM_WIDTH) {
      throw new IllegalArgumentException(
          "beamWidth must be positive and less than or equal to "
              + MAXIMUM_BEAM_WIDTH
              + "; beamWidth="
              + beamWidth);
    }
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    if (numMergeWorkers == 1 && mergeExec != null) {
      throw new IllegalArgumentException(
          "No executor service is needed as we'll use single thread to merge");
    }
    this.numMergeWorkers = numMergeWorkers;
    if (mergeExec != null) {
      this.mergeExec = new TaskExecutor(mergeExec);
    } else {
      this.mergeExec = null;
    }
    this.hnswWriteParams = hnswWriteParams;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99HnswVectorsWriter(
        state,
        this.maxConn,
        this.beamWidth,
        flatVectorsFormat.fieldsWriter(state),
        this.numMergeWorkers,
        this.mergeExec,
        this.hnswWriteParams.tinySegmentsThreshold(),
        this.hnswWriteParams.graphWriteVersion());
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene99NativeHnswVectorsReader(
        new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state)));
  }

  /**
   * A {@link KnnVectorsReader} that wraps {@link Lucene99HnswVectorsReader}, returned by {@link
   * Lucene99NativeHnswVectorsFormat#fieldsReader}. This type is distinct from {@link
   * Lucene99HnswVectorsReader} so callers can confirm via instanceof that the native format's SPI
   * entry was resolved rather than the stock Lucene one.
   */
  public static final class Lucene99NativeHnswVectorsReader extends KnnVectorsReader
      implements HnswGraphProvider {
    private final Lucene99HnswVectorsReader delegate;

    Lucene99NativeHnswVectorsReader(Lucene99HnswVectorsReader delegate) {
      this.delegate = delegate;
    }

    @Override
    public void checkIntegrity() throws IOException {
      this.delegate.checkIntegrity();
    }

    @Override
    @Nullable
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
      return this.delegate.getFloatVectorValues(field);
    }

    @Override
    @Nullable
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
      return this.delegate.getByteVectorValues(field);
    }

    @Override
    public void search(
        String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
        throws IOException {
      this.delegate.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(
        String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
        throws IOException {
      this.delegate.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public HnswGraph getGraph(String field) throws IOException {
      return this.delegate.getGraph(field);
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo info) {
      return this.delegate.getOffHeapByteSize(info);
    }

    @Override
    public void close() throws IOException {
      this.delegate.close();
    }
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return VectorFieldSpecification.MAX_DIMENSIONS;
  }

  @Override
  public String toString() {
    return "Lucene99NativeHnswVectorsFormat(name=Lucene99HnswVectorsFormat, maxConn="
        + this.maxConn
        + ", beamWidth="
        + this.beamWidth
        + ", flatVectorFormat="
        + flatVectorsFormat
        + ")";
  }
}
