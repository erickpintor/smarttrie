package smarttrie.atoms

import java.nio.ByteBuffer
import smarttrie.io._
import smarttrie.lang._

final case class Key(internalBuffer: ByteBuffer) extends AnyVal with Ordered[Key] {
  @inline def size: Int = internalBuffer.remaining()
  @inline def buf: ByteBuffer = internalBuffer.duplicate().asReadOnlyBuffer()
  @inline def compare(that: Key): Int = internalBuffer.compareTo(that.internalBuffer)
  override def toString: String = buf.toUTF8String
}

object Key {
  implicit object KeyCodec extends Codec[Key] {
    def size(key: Key): Int = key.size
    def encode(key: Key, buf: ByteBuffer): Unit = buf.put(key.buf)
    def decode(buf: ByteBuffer): Key = Key(buf)
  }
}

final case class Value(internalBuffer: ByteBuffer) extends AnyVal {
  @inline def size: Int = internalBuffer.remaining()
  @inline def buf: ByteBuffer = internalBuffer.duplicate().asReadOnlyBuffer()
  override def toString: String = buf.toUTF8String
}

object Value {
  implicit object ValueCodec extends Codec[Value] {
    def size(value: Value): Int = value.size
    def encode(value: Value, buf: ByteBuffer): Unit = buf.put(value.buf)
    def decode(buf: ByteBuffer): Value = Value(buf)
  }
}

final case class KeyValue(key: Key, value: Value)

object KeyValue {

  implicit object KeyValueCodec extends Codec[KeyValue] {

    def size(kv: KeyValue): Int =
      8 + Codec.size(kv.key) + Codec.size(kv.value)

    def encode(kv: KeyValue, buf: ByteBuffer): Unit =
      buf
        .putInt(kv.key.size)
        .putInt(kv.value.size)
        .put(kv.key.buf)
        .put(kv.value.buf)

    def decode(buf: ByteBuffer): KeyValue = {
      val kenLen = buf.getInt()
      val valLen = buf.getInt()
      val keyLim = buf.position() + kenLen
      val valLim = keyLim + valLen
      val key = buf.duplicate().limit(keyLim)
      val value = buf.duplicate().position(keyLim).limit(valLim)
      buf.position(valLim) // consume this key value pair from the buffer
      KeyValue(Codec.decode(key).as[Key], Codec.decode(value).as[Value])
    }
  }
}
