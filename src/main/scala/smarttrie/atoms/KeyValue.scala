package smarttrie.atoms

import java.nio.ByteBuffer
import java.util
import smarttrie.io._

object Key {

  implicit object KeyCodec extends Codec[Key] {

    def encode(key: Key, out: Encoder.Output): Unit =
      out
        .writeInt(key.data.length)
        .writeBytes(key.data)

    def decode(input: Decoder.Input): Key =
      Key(input.readBytes(input.readInt))
  }
}

final case class Key(data: Array[Byte]) extends Ordered[Key] {

  def compare(that: Key): Int = {
    val thisData = ByteBuffer.wrap(data)
    val thatData = ByteBuffer.wrap(that.data)
    thisData.compareTo(thatData)
  }

  override def hashCode(): Int =
    31 * util.Arrays.hashCode(data)

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Key => util.Arrays.equals(data, that.data)
      case _         => false
    }

  override def toString: String =
    s"Key(${data.map("%02X".format(_)).mkString})"
}

object Value {

  implicit object ValueCodec extends Codec[Value] {

    def encode(value: Value, out: Encoder.Output): Unit =
      out
        .writeInt(value.data.length)
        .writeBytes(value.data)

    def decode(input: Decoder.Input): Value =
      Value(input.readBytes(input.readInt))
  }
}

final case class Value(data: Array[Byte]) {

  override def hashCode(): Int =
    13 * util.Arrays.hashCode(data)

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Value => util.Arrays.equals(data, that.data)
      case _           => false
    }

  override def toString: String =
    s"Value(${data.map("%02X".format(_)).mkString})"
}
