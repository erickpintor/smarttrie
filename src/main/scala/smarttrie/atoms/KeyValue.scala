package smarttrie.atoms

import io.netty.buffer.ByteBuf
import smarttrie.io._
import smarttrie.lang._

object Key {

  def apply(buf: ByteBuf): Key =
    new Key(buf.asReadOnly())

  implicit object KeyCodec extends Codec[Key] {

    def encode(key: Key, out: Encoder.Output): Unit =
      out
        .writeInt(key.buf.readableBytes())
        .writeBuf(key.buf.duplicate())

    def decode(input: Decoder.Input): Key =
      Key(input.readBytes(input.readInt()))
  }
}

final case class Key(buf: ByteBuf) extends AnyVal with Ordered[Key] {
  def compare(that: Key): Int = buf.compareTo(that.buf)
  override def toString: String = buf.toUTF8String
}

object Value {

  def apply(buf: ByteBuf): Value =
    new Value(buf.asReadOnly())

  implicit object ValueCodec extends Codec[Value] {

    def encode(value: Value, out: Encoder.Output): Unit =
      out
        .writeInt(value.buf.readableBytes())
        .writeBuf(value.buf.duplicate())

    def decode(input: Decoder.Input): Value =
      Value(input.readBytes(input.readInt()))
  }
}

final case class Value(buf: ByteBuf) extends AnyVal {
  override def toString: String = buf.toUTF8String
}
