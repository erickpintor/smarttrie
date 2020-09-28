package smarttrie.atoms

import smarttrie.io._

sealed trait Command

object Command {
  final case class Get(key: Key) extends Command
  final case class Remove(key: Key) extends Command
  final case class Set(key: Key, value: Value) extends Command

  implicit object CommandCodec extends Codec[Command] {

    def encode(value: Command, out: Encoder.Output): Unit =
      value match {
        case Get(key)        => out.writeByte(0).write(key)
        case Remove(key)     => out.writeByte(1).write(key)
        case Set(key, value) => out.writeByte(2).write(key).write(value)
      }

    def decode(input: Decoder.Input): Command =
      input.readByte() match {
        case 0 => Get(input.read[Key])
        case 1 => Remove(input.read[Key])
        case 2 => Set(input.read[Key], input.read[Value])
      }
  }
}
