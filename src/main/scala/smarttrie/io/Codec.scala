package smarttrie.io

import io.netty.buffer.{ByteBuf, CompositeByteBuf, Unpooled}
import scala.util.Try

trait Encoder[-A] {
  def encode(value: A, out: Encoder.Output): Unit
}

object Encoder {

  trait Output {
    def write[A: Encoder](value: A): Output
    def writeByte(n: Byte): Output
    def writeInt(n: Int): Output
    def writeBuf(buf: ByteBuf): Output
  }

  final class ByteBufOutput(private[this] val cmp: CompositeByteBuf) extends Output {
    private[this] var out = cmp.alloc().buffer()

    def write[A](value: A)(implicit enc: Encoder[A]): Output = {
      enc.encode(value, this)
      this
    }

    def writeByte(n: Byte): Output = {
      out.writeByte(n)
      this
    }

    def writeInt(n: Int): Output = {
      out.writeInt(n)
      this
    }

    def writeBuf(buf: ByteBuf): Output = {
      flush()
      if (buf.readableBytes() > 0) {
        cmp.addComponent(true, buf)
      }
      this
    }

    def flush(): Unit =
      if (out.readableBytes() > 0) {
        cmp.addComponent(true, out)
        out = out.alloc().buffer()
      }
  }
}

trait Decoder[+A] {
  def decode(input: Decoder.Input): A
}

object Decoder {

  trait Input {
    def read[A: Decoder]: A
    def readByte(): Byte
    def readInt(): Int
    def readBytes(n: Int): ByteBuf
  }

  final class ByteBufReader(buf: ByteBuf) extends Input {
    def read[A](implicit dec: Decoder[A]): A = dec.decode(this)
    def readByte(): Byte = buf.readByte()
    def readInt(): Int = buf.readInt()
    def readBytes(n: Int): ByteBuf = buf.readBytes(n)
  }

  final class API(buf: ByteBuf) {
    def as[A](implicit dec: Decoder[A]): A = tryAs[A].get
    def tryAs[A](implicit dec: Decoder[A]): Try[A] =
      Try(dec.decode(new ByteBufReader(buf)))
  }
}

trait Codec[A] extends Encoder[A] with Decoder[A]

object Codec {

  def writer(f: Encoder.Output => Any): ByteBuf = {
    val buf = Unpooled.compositeBuffer()
    val out = new Encoder.ByteBufOutput(buf)
    f(out)
    out.flush()
    buf
  }

  def encode[A](value: A)(implicit enc: Encoder[A]): ByteBuf =
    writer { enc.encode(value, _) }

  def decode(bytes: Array[Byte]): Decoder.API =
    decode(Unpooled.wrappedBuffer(bytes))

  def decode(buf: ByteBuf): Decoder.API =
    new Decoder.API(buf)
}
