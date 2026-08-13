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

package com.xgen.mongot.index.lucene.quantization;

import static com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat.DEFAULT_HNSW_GRAPH_THRESHOLD;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN;

import com.google.auto.service.AutoService;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.lucene.vector.HnswWriteParams;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.hnsw.HnswGraph;

// This file is copied from Lucene 9.11.1 branch
// https://github.com/apache/lucene/blob/releases/lucene/9.11.1/lucene/codecs/src/java/org/apache/lucene/codecs/bitvectors/HnswBitVectorsFormat.java
// There are some changes made after copying the file from Lucene and they're marked by comments

/**
 * Encodes bit vector values into an associated graph connecting the documents having values. The
 * graph is used to power HNSW search. The format consists of two files, and uses {@link
 * Lucene99FlatVectorsFormat} to store the actual vectors, but with a custom scorer implementation:
 * For details on graph storage and file extensions, see {@link Lucene99HnswVectorsFormat}.
 *
 * @lucene.experimental
 */
@AutoService(KnnVectorsFormat.class)
public final class Mongot01042HnswBitVectorsFormat extends KnnVectorsFormat {

  public static final String NAME = "Mongot01042HnswBitVectorsFormat";

  /**
   * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
   * {@link Lucene99HnswVectorsFormat#DEFAULT_MAX_CONN}. See {@link HnswGraph} for more details.
   */
  private final int maxConn;

  /**
   * The number of candidate neighbors to track while searching the graph for each newly inserted
   * node. Defaults to {@link Lucene99HnswVectorsFormat#DEFAULT_BEAM_WIDTH}. See {@link HnswGraph}
   * for details.
   */
  private final int beamWidth;

  /** The format for storing, reading, merging vectors on disk */
  private final FlatVectorsFormat flatVectorsFormat;

  /** Number of merge worker threads; values above 1 require {@link #mergeExec}. */
  private final int numMergeWorkers;

  // [Changed from Lucene]
  // Wrapped the task executor object in an Optional to avoid dealing with nulls
  /** Executor used by the vector writers this format creates to parallelize merges. */
  private final Optional<TaskExecutor> mergeExec;

  /** Tiny-segment graph threshold and graph write version; see {@link HnswWriteParams}. */
  private final HnswWriteParams hnswWriteParams;

  /** Constructs a format using default graph construction parameters */
  public Mongot01042HnswBitVectorsFormat() {
    this(
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NUM_MERGE_WORKER,
        Optional.empty(),
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
   *     generated by this format to do the merge
   * @param hnswWriteParams the tiny-segment graph threshold and graph write version
   */
  public Mongot01042HnswBitVectorsFormat(
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      Optional<ExecutorService> mergeExec,
      HnswWriteParams hnswWriteParams) {
    super(NAME);
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
    if (numMergeWorkers == 1 && mergeExec.isPresent()) {
      throw new IllegalArgumentException(
          "No executor service is needed as we'll use single thread to merge");
    }
    this.numMergeWorkers = numMergeWorkers;
    this.mergeExec = mergeExec.map(TaskExecutor::new);
    this.flatVectorsFormat = new Lucene99FlatVectorsFormat(new FlatBitVectorsScorer());
    this.hnswWriteParams = hnswWriteParams;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new FlatBitVectorsWriter(
        new Lucene99HnswVectorsWriter(
            state,
            this.maxConn,
            this.beamWidth,
            this.flatVectorsFormat.fieldsWriter(state),
            this.numMergeWorkers,
            this.mergeExec.orElse(null),
            this.hnswWriteParams.tinySegmentsThreshold(),
            this.hnswWriteParams.graphWriteVersion()));
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene99HnswVectorsReader(state, this.flatVectorsFormat.fieldsReader(state));
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    // [Changed from Lucene]
    // Changed the max allowed vector dimensions.
    return VectorFieldSpecification.MAX_DIMENSIONS;
  }

  @Override
  public String toString() {
    return NAME
        + "(name="
        + NAME
        + ", maxConn="
        + this.maxConn
        + ", beamWidth="
        + this.beamWidth
        + ", flatVectorFormat="
        + this.flatVectorsFormat
        + ")";
  }

  private static class FlatBitVectorsWriter extends KnnVectorsWriter {
    private final KnnVectorsWriter delegate;

    public FlatBitVectorsWriter(KnnVectorsWriter delegate) {
      this.delegate = delegate;
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      this.delegate.mergeOneField(fieldInfo, mergeState);
    }

    @Override
    public void finish() throws IOException {
      this.delegate.finish();
    }

    @Override
    public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
      if (fieldInfo.getVectorEncoding() != VectorEncoding.BYTE) {
        throw new IllegalArgumentException(NAME + " only supports BYTE encoding");
      }
      return this.delegate.addField(fieldInfo);
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
      this.delegate.flush(maxDoc, sortMap);
    }

    @Override
    public void close() throws IOException {
      this.delegate.close();
    }

    @Override
    public long ramBytesUsed() {
      return this.delegate.ramBytesUsed();
    }
  }
}
