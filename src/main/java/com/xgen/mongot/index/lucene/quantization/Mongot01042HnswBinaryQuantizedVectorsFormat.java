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

// NOTE(corecursion): Copied from: Lucene99HnswScalarQuantizedVectorsFormat.java

package com.xgen.mongot.index.lucene.quantization;

import static com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat.DEFAULT_HNSW_GRAPH_THRESHOLD;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN;

import com.google.auto.service.AutoService;
import com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.hnsw.HnswGraph;

/**
 * Lucene 9.9 vector format, which encodes numeric vector values into an associated graph connecting
 * the documents having values. The graph is used to power HNSW search. The format consists of two
 * files, and uses {@link Lucene99ScalarQuantizedVectorsFormat} to store the actual vectors: For
 * details on graph storage and file extensions, see {@link Lucene99HnswVectorsFormat}.
 *
 * @lucene.experimental
 */
@AutoService(KnnVectorsFormat.class)
public class Mongot01042HnswBinaryQuantizedVectorsFormat extends KnnVectorsFormat {

  public static final String NAME = "Mongot01042HnswBinaryQuantizedVectorsFormat";

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

  private final Optional<TaskExecutor> mergeExec;
  private final int numMergeWorkers;
  private final int tinySegmentsThreshold;

  /** Constructs a format using default graph construction parameters */
  public Mongot01042HnswBinaryQuantizedVectorsFormat() {
    this(
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NUM_MERGE_WORKER,
        Optional.empty(),
        DEFAULT_HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   */
  public Mongot01042HnswBinaryQuantizedVectorsFormat(int maxConn, int beamWidth) {
    this(
        maxConn,
        beamWidth,
        DEFAULT_NUM_MERGE_WORKER,
        Optional.empty(),
        DEFAULT_HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters and scalar quantization.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param numMergeWorkers number of workers (threads) that will be used when doing merge. If
   *     larger than 1, a non-null {@link ExecutorService} must be passed as mergeExec
   * @param mergeExec the {@link ExecutorService} that will be used by ALL vector writers that are
   *     generated by this format to do the merge
   * @param tinySegmentsThreshold segments whose vector count is below this skip HNSW graph
   *     construction; see {@link Lucene99NativeHnswVectorsFormat#DEFAULT_HNSW_GRAPH_THRESHOLD}
   */
  public Mongot01042HnswBinaryQuantizedVectorsFormat(
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      Optional<ExecutorService> mergeExec,
      int tinySegmentsThreshold) {
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
    if (mergeExec.isPresent()) {
      this.mergeExec = Optional.of(new TaskExecutor(mergeExec.get()));
    } else {
      this.mergeExec = Optional.empty();
    }
    this.flatVectorsFormat = new Mongot01042BinaryQuantizedFlatVectorsFormat();
    this.tinySegmentsThreshold = tinySegmentsThreshold;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99HnswVectorsWriter(
        state,
        this.maxConn,
        this.beamWidth,
        this.flatVectorsFormat.fieldsWriter(state),
        this.numMergeWorkers,
        this.mergeExec.orElse(null),
        this.tinySegmentsThreshold);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene99HnswVectorsReader(state, this.flatVectorsFormat.fieldsReader(state));
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return 1024;
  }

  @Override
  public String toString() {
    return "Mongot01042HnswBinaryQuantizedVectorsFormat("
        + "name=Mongot01042HnswBinaryQuantizedVectorsFormat, maxConn="
        + this.maxConn
        + ", beamWidth="
        + this.beamWidth
        + ", flatVectorFormat="
        + this.flatVectorsFormat
        + ")";
  }
}
