package smarttrie.io

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream
}

trait Encoder[-A] {
  def encode(value: A, out: Encoder.Output): Unit
}

object Encoder {

  trait Output {
    def write[A: Encoder](value: A): Output
    def writeInt(n: Int): Output
    def writeByte(n: Byte): Output
    def writeBytes(arr: Array[Byte]): Output
  }

  final class DefaultOutput(private[this] val out: DataOutputStream) extends Output {

    def write[A](value: A)(implicit enc: Encoder[A]): Output = {
      enc.encode(value, this)
      this
    }

    def writeInt(n: Int): Output = {
      out.writeInt(n)
      this
    }

    def writeByte(n: Byte): Output = {
      out.write(n)
      this
    }

    def writeBytes(arr: Array[Byte]): Output = {
      out.write(arr)
      this
    }
  }
}

trait Decoder[A] {
  def decode(input: Decoder.Input): A
}

object Decoder {

  trait Input {
    def read[A: Decoder]: A
    def readInt: Int
    def readByte: Byte
    def readBytes(n: Int): Array[Byte]
    def hasMoreBytes: Boolean
  }

  final class DefaultInput(in: DataInputStream) extends Input {
    def read[A](implicit dec: Decoder[A]): A = dec.decode(this)
    def readInt: Int = in.readInt()
    def readByte: Byte = in.readByte()
    def readBytes(n: Int): Array[Byte] = in.readNBytes(n)
    def hasMoreBytes: Boolean = in.available() > 0
  }

  final class API(in: Decoder.Input) {
    def as[A](implicit dec: Decoder[A]): A =
      dec.decode(in)
  }
}

trait Codec[A] extends Encoder[A] with Decoder[A]

object Codec {

  def writer(f: Encoder.Output => Any): Array[Byte] = {
    val arr = new ByteArrayOutputStream()
    val out = new Encoder.DefaultOutput(new DataOutputStream(arr))
    f(out)
    arr.toByteArray
  }

  def encode[A](value: A)(implicit enc: Encoder[A]): Array[Byte] =
    writer { enc.encode(value, _) }

  def reader(bytes: Array[Byte]): Decoder.Input = {
    val in = new ByteArrayInputStream(bytes)
    new Decoder.DefaultInput(new DataInputStream(in))
  }

  def decode(bytes: Array[Byte]): Decoder.API =
    new Decoder.API(reader(bytes))
}
