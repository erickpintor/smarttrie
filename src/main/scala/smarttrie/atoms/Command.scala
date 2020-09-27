package smarttrie.atoms

import java.nio.ByteBuffer
import smarttrie.io._

sealed trait Command

object Command {
  final case class Get(key: Key) extends Command
  final case class Remove(key: Key) extends Command
  final case class Set(key: Key, value: Value) extends Command

  implicit object CommandCodec extends Codec[Command] {
    def size(cmd: Command): Int =
      cmd match {
        case Get(k)    => 1 + Codec.size(k)
        case Remove(k) => 1 + Codec.size(k)
        case Set(k, v) => 1 + Codec.size(KeyValue(k, v))
      }

    def encode(cmd: Command, buf: ByteBuffer): Unit =
      cmd match {
        case Get(k)    => Codec.encode(k, buf.put(0: Byte))
        case Remove(k) => Codec.encode(k, buf.put(1: Byte))
        case Set(k, v) => Codec.encode(KeyValue(k, v), buf.put(2: Byte))
      }

    def decode(buf: ByteBuffer): Command =
      buf.get() match {
        case 0 => Get(Key(buf))
        case 1 => Remove(Key(buf))
        case 2 =>
          val kv = Codec.decode(buf).as[KeyValue]
          Set(kv.key, kv.value)
      }
  }
}
