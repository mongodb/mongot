# Vector Quantization

`com.xgen.mongot.index.lucene.quantization`

Vectors indexed for vector search can take up a large amount of space, both in memory and on
durable storage. Quantization is a form of compression used to reduce the size of the vectors.

## Automatically-Quantized Vectors

A user can provide full-fidelity (float32) vectors to be ingested by MongoDB while requesting for
MongoDB to automatically quantize the vectors to save space in main memory.

Automatic quantization is controlled by the `quantization` setting which can be either `scalar`
or `binary` in the index definition of a vector search index.

With this feature, both the quantized vectors and the original, full-fidelity, vectors will be
kept by mongot on durable storage. Additionally, another copy of the original, full-fidelity,
vectors will be kept by mongod. However, only the quantized dataset will be fully loaded by
mongot into main memory, saving up to 75% of the space for `scalar` quantization and up to 97% of
the space for `binary` quantization.

### Scalar Quantization

Scalar quantization, enabled by setting the `quantization` to `scalar` in the index definition
for a vector search index, accepts vectors with 32-bit (float32) elements that are automatically
quantized into 8-bit elements, saving three bytes per element.

Scalar quantization is a feature already built into Lucene and enabled in mongot's `LuceneCodec`,
so there are no files in this mongot quantization library for implementing scalar quantization.

### Binary Quantization

Binary Quantization, enabled by setting the `quantization` to `binary` in the index definition
for a vector search index, accepts vectors with 32-bit (float32) elements that are automatically
quantized into 1-bit elements, saving 31 bits per element.

A binary quantization feature is being developed for Lucene but is not yet available from Lucene
(as of December 2024). MongoDB provides binary quantization as an enhancement to Lucene developed
by MongoDB in-house.

The main class for the binary quantization feature is:
[Mongot01042HnswBinaryQuantizedVectorsFormat](Mongot01042HnswBinaryQuantizedVectorsFormat.java).

Other important BQ classes are:
[Mongot01042BinaryQuantizedFlatVectorsFormat](Mongot01042BinaryQuantizedFlatVectorsFormat.java),
[Mongot01042BinaryQuantizedFlatVectorsReader](Mongot01042BinaryQuantizedFlatVectorsReader.java),
and [Mongot01042BinaryQuantizedFlatVectorsWriter](Mongot01042BinaryQuantizedFlatVectorsWriter.java),
used for managing the new file format that stores binary quantized vectors.

These two classes calculate vector scores.
[BinaryQuantizedFlatVectorsScorer](BinaryQuantizedFlatVectorsScorer.java) does very fast Hamming
distance scoring during HNSW traversal.
[BinaryQuantizedVectorRescorer](BinaryQuantizedVectorRescorer.java) does full-fidelity rescoring
on query result candidates after the HNSW algorithm has completed.

[BinaryQuantizer](BinaryQuantizer.java) quantizes a full-fidelity vector with float32 elements
into a quantized vector with 1-bit elements.

[OffHeapQuantizedByteVectorValues](OffHeapQuantizedByteVectorValues.java) packs and unpacks the
bits inside quantized vectors. Lucene sometimes refers to bit packing as compression and there is
a `compress` flag used in the code to enable/disable this capability. Lucene currently only uses
compression for 4-bit scalar quantization, but MongoDB extends this capability to work with 1-bit
binary quantization. When `compress` is false, elements are stored one dimension per byte inside
a Java `byte` array. When `compress` is true, which is always the case with MongoDB binary
quantization, elements are stored eight bits per byte.


[DequantizedFloatVectorValues](DequantizedFloatVectorValues.java) and
[DequantizedByteVectorValues](DequantizedByteVectorValues.java) are trivial VectorValues classes
for converting binary-quantized 1-bit elements to `float` and `byte`, respectively. They are used
by [BinaryQuantizedVectorRescorer](BinaryQuantizedVectorRescorer.java).

## Pre-Quantized Vectors

A user can provide pre-quantized vectors to be ingested by MongoDB. The ingested, pre-quantized,
vectors seen by MongoDB are presumably smaller than the original (full-fidelity) vectors that
existed elsewhere and so space will be saved inside MongoDB.

Pre-quantized vectors may be used only if the `quantization` setting in the index definition is
`none` (or is missing which will default it to `none`). The `quantization` setting is for
automatic quantization, not for pre-quantized vectors.

The main class for the ingestion of pre-quantized vectors feature is:
[Mongot01042HnswBitVectorsFormat](Mongot01042HnswBitVectorsFormat.java).

[FlatBitVectorsScorer](FlatBitVectorsScorer.java) calculates scoring for the HNSW algorithm.
