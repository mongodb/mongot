package com.xgen.mongot.server.grpc;

import io.grpc.KnownLength;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.bson.RawBsonDocument;

/** This marshaller will be called by gRPC libraries to encode/decode {@link RawBsonDocument}. */
public class RawBsonMarshaller implements MethodDescriptor.Marshaller<RawBsonDocument> {

  @Override
  public InputStream stream(RawBsonDocument value) {
    try {
      ByteBuffer buffer = value.getByteBuffer().asNIO().duplicate();
      return new KnownLengthStream(buffer);
    } catch (Exception e) {
      throw Status.INTERNAL
          .withDescription("cannot encode RawBsonDocument")
          .withCause(e)
          .asRuntimeException();
    }
  }

  @Override
  public RawBsonDocument parse(InputStream stream) {
    try {
      return new RawBsonDocument(stream.readAllBytes());
    } catch (Exception e) {
      throw Status.INTERNAL
          .withDescription("cannot decode RawBsonDocument")
          .withCause(e)
          .asRuntimeException();
    }
  }

  /**
   * Zero-copy InputStream that reads directly from a {@link ByteBuffer} and reports its length via
   * {@link KnownLength#available()}.
   */
  private static final class KnownLengthStream extends InputStream implements KnownLength {

    private final ByteBuffer buffer;

    /**
     * Creates an {@link InputStream} with a known-length from a {@link ByteBuffer}.
     *
     * @param buffer the ByteBuffer containing the data to be read by this InputStream. The buffer
     *     may be on-heap or native, writable or read-only. This stream will modify the given
     *     buffer's position, but not its content. If you plan to read the buffer multiple times,
     *     you should call {@link ByteBuffer#duplicate} before passing it to this constructor.
     */
    public KnownLengthStream(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public int available() {
      return this.buffer.remaining();
    }

    @Override
    public int read() {
      return this.buffer.hasRemaining() ? (this.buffer.get() & 0xFF) : -1;
    }

    @Override
    public int read(byte[] bytes, int off, int len) {
      if (!this.buffer.hasRemaining()) {
        return -1;
      }
      int bytesRead = Math.min(len, this.buffer.remaining());
      this.buffer.get(bytes, off, bytesRead);
      return bytesRead;
    }

    @Override
    public long skip(long n) {
      int skipped = (int) Math.min(n, this.buffer.remaining());
      this.buffer.position(this.buffer.position() + skipped);
      return skipped;
    }
  }
}
