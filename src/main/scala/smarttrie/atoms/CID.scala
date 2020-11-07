package smarttrie.atoms

import smarttrie.io._

final case class CID(toInt: Int) extends AnyVal with Ordered[CID] {
  @inline def compare(that: CID): Int = toInt.compareTo(that.toInt)
  @inline override def toString: String = s"CID($toInt)"
}

object CID {
  val Null = CID(-1)

  implicit object CIDCodec extends Codec[CID] {
    def encode(cid: CID, out: Encoder.Output): Unit = out.writeInt(cid.toInt)
    def decode(in: Decoder.Input): CID = CID(in.readInt)
  }
}
