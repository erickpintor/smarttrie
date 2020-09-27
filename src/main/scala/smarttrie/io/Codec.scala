package smarttrie.io

import java.nio.ByteBuffer
import scala.util.Try

trait Encoder[-A] {
  def size(value: A): Int
  def encode(value: A, buf: ByteBuffer): Unit
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

  def size[A](value: A)(implicit enc: Encoder[A]): Int =
    enc.size(value)

  def encode[A: Encoder](value: A): ByteBuffer = {
    val buf = ByteBuffer.allocate(size(value))
    encode(value, buf)
    buf.flip()
  }

  def encode[A](value: A, buf: ByteBuffer)(implicit enc: Encoder[A]): Unit =
    enc.encode(value, buf)

  def decode(bytes: Array[Byte]): Decoder.API =
    decode(ByteBuffer.wrap(bytes))

  def decode(buf: ByteBuffer): Decoder.API =
    new Decoder.API(buf)
}
