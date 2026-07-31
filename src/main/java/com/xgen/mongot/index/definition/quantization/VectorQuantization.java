package com.xgen.mongot.index.definition.quantization;

/** Supported vector field quantization types. */
public enum VectorQuantization {
  // no quantization
  NONE,

  // uses the Lucene float32 -> int7 quantization as the backend
  SCALAR,

  // uses MongoDB's own float32 or int8 -> uint1 quantization as the backend
  BINARY
}
