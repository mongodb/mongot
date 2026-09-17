package com.xgen.mongot.server.grpc;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.server.message.MessageHeader;
import com.xgen.mongot.server.message.MessageMessage;
import com.xgen.mongot.server.message.OpCode;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This marshaller will be called by gRPC libraries to encode/decode {@link MessageMessage}s.
 *
 * <p>The encoding/decoding logic will follow the spec <a
 * href="https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst">here</a>.
 */
public class MessageMarshaller implements MethodDescriptor.Marshaller<MessageMessage> {
  // Netty 4.2 changed the default ByteBufAllocator from pooled to adaptive. This marshaller is
  // invoked off the event loop by gRPC to encode every outbound OP_MSG, so pin the pooled allocator
  // explicitly to preserve the pooling behavior mongot relied on under Netty 4.1 rather than
  // silently inherit the new adaptive default with this CVE bump. See CLOUDP-439486.
  @VisibleForTesting static final ByteBufAllocator ALLOCATOR = PooledByteBufAllocator.DEFAULT;

  @Override
  public InputStream stream(MessageMessage value) {
    try {
      ByteBuf buf = value.toByteBuf(ALLOCATOR);
      return new ByteBufInputStream(buf, true);
    } catch (Throwable t) {
      throw Status.INTERNAL
          .withDescription("cannot encode OP_MSG")
          .withCause(t)
          .asRuntimeException();
    }
  }

  @Override
  public MessageMessage parse(InputStream stream) {
    try {
      MessageHeader messageHeader = parseMessageHeader(stream);
      checkArg(
          messageHeader.opCode() == OpCode.MSG, "unknown op code: %s", messageHeader.opCode().code);
      ByteBuf messageBody =
          Unpooled.wrappedBuffer(
              stream.readNBytes(messageHeader.messageLength() - MessageHeader.SIZE_IN_BYTES));
      try {
        return MessageMessage.fromBytes(messageHeader, messageBody);
      } finally {
        messageBody.release();
      }
    } catch (Throwable t) {
      throw Status.INTERNAL
          .withDescription("cannot decode OP_MSG")
          .withCause(t)
          .asRuntimeException();
    }
  }

  private static MessageHeader parseMessageHeader(InputStream stream) throws IOException {
    byte[] bytes = stream.readNBytes(MessageHeader.SIZE_IN_BYTES);
    checkArg(
        bytes.length == MessageHeader.SIZE_IN_BYTES,
        "expected to read 16 bytes, read: %s",
        bytes.length);

    // According to the spec of Wire Protocol, all integers in the header use little-endian byte
    // order. However, JVM is always big-endian whether the underlying OS/Hardware is big-endian or
    // little-endian.
    var buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

    int messageSize = buf.getInt();
    checkArg(
        messageSize >= MessageHeader.SIZE_IN_BYTES,
        "expected a size of 16 or greater, got: %s",
        messageSize);

    int requestID = buf.getInt();
    int responseTo = buf.getInt();
    OpCode opCode = OpCode.fromCode(buf.getInt());
    return new MessageHeader(messageSize, requestID, responseTo, opCode);
  }
}
