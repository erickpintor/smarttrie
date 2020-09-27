package smarttrie.io

import java.nio.ByteBuffer
import scala.util.Try

trait Encoder[-A] {
  def encode(value: A, buf: ByteBuffer): ByteBuffer
  def size(value: A): Int
}

trait Decoder[+A] {
  def decode(buf: ByteBuffer): A
}

object Decoder {
  final class API(buf: ByteBuffer) {
    def tryAs[A](implicit dec: Decoder[A]): Try[A] = Try(dec.decode(buf))
    def as[A](implicit dec: Decoder[A]): A = tryAs[A].get
  }
}

trait Codec[A] extends Encoder[A] with Decoder[A]

object Codec {

  def encode[A](value: A)(implicit enc: Encoder[A]): ByteBuffer = {
    val size = enc.size(value)
    val buf = ByteBuffer.allocate(size)
    enc.encode(value, buf).flip()
  }

  def decode(buf: ByteBuffer): Decoder.API = new Decoder.API(buf)
}
