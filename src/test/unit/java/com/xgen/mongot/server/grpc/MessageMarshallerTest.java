package com.xgen.mongot.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.server.message.MessageMessage;
import com.xgen.mongot.server.message.OpCode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class MessageMarshallerTest {
  /** Creates an example MessageMessage and returns the corresponding byte array. */
  private static ByteBuffer getBytesForExampleOpMsg() {
    ByteBuf buf =
        MessageMessage.forResponse(233, 0, new ArrayList<>())
            .getOutboundMessage(new BsonDocument().append("hello", BsonBoolean.TRUE))
            .toByteBuf(ByteBufAllocator.DEFAULT);
    byte[] msgBytes = new byte[buf.readableBytes()];
    buf.readBytes(msgBytes);
    buf.release();
    return ByteBuffer.wrap(msgBytes).order(ByteOrder.LITTLE_ENDIAN);
  }

  @Test
  public void testBasicEncodeAndDecode() throws IOException {
    var buffer = getBytesForExampleOpMsg();
    MessageMarshaller messageMarshaller = new MessageMarshaller();
    MessageMessage message = messageMarshaller.parse(new ByteArrayInputStream(buffer.array()));
    try (InputStream stream = messageMarshaller.stream(message)) {
      Assert.assertArrayEquals(buffer.array(), stream.readAllBytes());
    }
  }

  @Test
  public void testNotEnoughBytes() {
    var buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(12);
    buffer.putInt(0xCAFE);
    buffer.putInt(0xFEED);
    var msgBytes = buffer.flip().array();
    MessageMarshaller messageMarshaller = new MessageMarshaller();
    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () -> messageMarshaller.parse(new ByteArrayInputStream(msgBytes)));
    assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
  }

  @Test
  public void testSizeTooSmall() {
    var buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(12);
    buffer.putInt(0xCAFE);
    buffer.putInt(0xFEED);
    buffer.putInt(0xFADE);
    var msgBytes = buffer.flip().array();
    MessageMarshaller messageMarshaller = new MessageMarshaller();
    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () -> messageMarshaller.parse(new ByteArrayInputStream(msgBytes)));
    assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
  }

  @Test
  public void testInvalidOpCode() {
    var buffer = getBytesForExampleOpMsg();
    buffer.putInt(12, OpCode.QUERY.code);
    MessageMarshaller messageMarshaller = new MessageMarshaller();
    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () -> messageMarshaller.parse(new ByteArrayInputStream(buffer.array())));
    assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
  }

  // CLOUDP-428627 regression: a forged BSON document length prefix (0x7FFFFFFF) must be rejected
  // cleanly rather than crashing the decode path (previously an unbounded `new byte[size]`
  // allocation threw OutOfMemoryError here too).
  @Test
  public void testAbsurdBsonLengthRejectedCleanly() {
    // 16-byte header (messageSize=25, requestId=1, responseTo=0, opCode=MSG) + 9-byte OP_MSG body
    // (flagBits=0, section kind=Body, forged BSON document length=0x7FFFFFFF).
    var buffer = ByteBuffer.allocate(25).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(25);
    buffer.putInt(1);
    buffer.putInt(0);
    buffer.putInt(OpCode.MSG.code);
    buffer.putInt(0);
    buffer.put((byte) 0x00);
    buffer.putInt(0x7FFFFFFF);
    var msgBytes = buffer.array();

    MessageMarshaller messageMarshaller = new MessageMarshaller();
    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () -> messageMarshaller.parse(new ByteArrayInputStream(msgBytes)));

    assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
  }
}
