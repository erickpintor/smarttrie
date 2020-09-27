package smarttrie.atoms

import java.nio.ByteBuffer
import smarttrie.io._

sealed trait Reply

object Reply {
  case object Error extends Reply
  case object NotFound extends Reply
  case class Data(value: Value) extends Reply

  implicit object MessageCodec extends Codec[Reply] {
    def size(msg: Reply): Int =
      msg match {
        case Error | NotFound => 1
        case Data(value)      => 1 + Codec.size(value)
      }

    def encode(msg: Reply, buf: ByteBuffer): Unit =
      msg match {
        case Error       => buf.put(0: Byte)
        case NotFound    => buf.put(1: Byte)
        case Data(value) => Codec.encode(value, buf.put(2: Byte))
      }

    def decode(buf: ByteBuffer): Reply =
      buf.get() match {
        case 0 => Error
        case 1 => NotFound
        case 2 => Data(Codec.decode(buf).as[Value])
      }
  }
}
