package smarttrie.atoms

import java.nio.ByteBuffer
import smarttrie.io.Codec

sealed trait Command

object Command {
  final case class Get(key: Key) extends Command
  final case class Set(key: Key, value: Value) extends Command

  implicit object GetCodec extends Codec[Get] {
    def size(get: Get): Int = get.key.size
    def encode(get: Get, buf: ByteBuffer): ByteBuffer = buf.put(get.key.buf)
    def decode(buf: ByteBuffer): Get = Get(Key(buf))
  }

  implicit object SetCodec extends Codec[Set] {
    // set length + key + value
    def size(set: Set): Int =
      4 + set.key.size + set.value.size

    def encode(set: Set, buf: ByteBuffer): ByteBuffer =
      buf
        .putInt(set.key.size)
        .put(set.key.buf)
        .put(set.value.buf)

    def decode(buf: ByteBuffer): Set = {
      val kenLen = buf.getInt()
      val key = buf.duplicate().limit(buf.position() + kenLen)
      val value = buf.position(buf.position() + kenLen)
      Set(Key(key), Value(value))
    }
  }

  implicit object CommandCodec extends Codec[Command] {
    // flag + command
    def size(cmd: Command): Int =
      1 + (cmd match {
        case c: Get => GetCodec.size(c)
        case c: Set => SetCodec.size(c)
      })

    def encode(cmd: Command, buf: ByteBuffer): ByteBuffer =
      cmd match {
        case c: Get => GetCodec.encode(c, buf.put(0: Byte))
        case c: Set => SetCodec.encode(c, buf.put(1: Byte))
      }

    def decode(buf: ByteBuffer): Command =
      buf.get() match {
        case 0 => GetCodec.decode(buf)
        case 1 => SetCodec.decode(buf)
      }
  }
}
