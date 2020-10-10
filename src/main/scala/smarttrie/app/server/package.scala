package smarttrie.app

import smarttrie.io._

package server {
  final case class CID(toInt: Int) extends AnyVal with Ordered[CID] {
    @inline def compare(that: CID): Int = toInt.compareTo(that.toInt)
    @inline def next: CID = CID(toInt + 1)
  }

  object CID {
    val Null = CID(-1)
    implicit object CIDCodec extends Codec[CID] {
      def encode(cid: CID, out: Encoder.Output): Unit = out.writeInt(cid.toInt)
      def decode(in: Decoder.Input): CID = CID(in.readInt)
    }
  }
}

package object server {
  type Request = Array[Byte]
  type Batch = Array[Request]
}
