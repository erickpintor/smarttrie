package smarttrie.atoms

import smarttrie.io._

sealed trait Reply

object Reply {
  case object Error extends Reply
  case object Null extends Reply
  case class Data(value: Value) extends Reply

  implicit object ReplyCodec extends Codec[Reply] {

    def encode(value: Reply, out: Encoder.Output): Unit =
      value match {
        case Error       => out.writeByte(0)
        case Null        => out.writeByte(1)
        case Data(value) => out.writeByte(2).write(value)
      }

    def decode(input: Decoder.Input): Reply =
      input.readByte() match {
        case 0 => Error
        case 1 => Null
        case 2 => Data(input.read[Value])
      }
  }
}
