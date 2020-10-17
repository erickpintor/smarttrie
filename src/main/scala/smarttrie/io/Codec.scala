package smarttrie.io

import io.netty.util.Recycler
import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream
}
import java.nio.ByteBuffer
import smarttrie.io.Encoder.ByteBufferOutput

trait Encoder[-A] {
  def encode(value: A, out: Encoder.Output): Unit
}

object Encoder {

  trait Output {
    def write[A: Encoder](value: A): Output
    def writeInt(n: Int): Output
    def writeLong(n: Long): Output
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

    def writeLong(n: Long): Output = {
      out.writeLong(n)
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

  final class ByteBufferOutput(private[this] val out: ByteBuffer) extends Output {

    def write[A](value: A)(implicit enc: Encoder[A]): Output = {
      enc.encode(value, this)
      this
    }

    def writeInt(n: Int): Output = {
      out.putInt(n)
      this
    }

    def writeLong(n: Long): Output = {
      out.putLong(n)
      this
    }

    def writeByte(n: Byte): Output = {
      out.put(n)
      this
    }

    def writeBytes(arr: Array[Byte]): Output = {
      out.put(arr)
      this
    }
  }

  final class SizeCountOutput(var count: Long = 0) extends Output {

    def write[A](value: A)(implicit enc: Encoder[A]): Output = {
      enc.encode(value, this)
      this
    }

    def writeInt(n: Int): Output = {
      count += 4
      this
    }

    def writeLong(n: Long): Output = {
      count += 8
      this
    }

    def writeByte(n: Byte): Output = {
      count += 1
      this
    }

    def writeBytes(arr: Array[Byte]): Output = {
      count += arr.length
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
    def readLong: Long
    def readByte: Byte
    def readBytes(n: Int): Array[Byte]
    def hasMoreBytes: Boolean
  }

  final class DefaultInput(in: DataInputStream) extends Input {
    def read[A](implicit dec: Decoder[A]): A = dec.decode(this)
    def readInt: Int = in.readInt()
    def readLong: Long = in.readLong()
    def readByte: Byte = in.readByte()
    def readBytes(n: Int): Array[Byte] = in.readNBytes(n)
    def hasMoreBytes: Boolean = in.available() > 0
  }

  final class ByteBufferInput(in: ByteBuffer) extends Input {
    def read[A](implicit dec: Decoder[A]): A = dec.decode(this)
    def readInt: Int = in.getInt()
    def readLong: Long = in.getLong()
    def readByte: Byte = in.get()
    def hasMoreBytes: Boolean = in.hasRemaining
    def readBytes(n: Int): Array[Byte] = {
      val arr = new Array[Byte](n)
      in.get(arr)
      arr
    }
  }

  final class API(in: Decoder.Input) {
    def as[A](implicit dec: Decoder[A]): A =
      dec.decode(in)
  }
}

trait Codec[A] extends Encoder[A] with Decoder[A]

object Codec {

  def sizeOf[A](value: A)(implicit enc: Encoder[A]): Long = {
    val size = new Encoder.SizeCountOutput()
    enc.encode(value, size)
    size.count
  }

  def encode[A](value: A)(implicit enc: Encoder[A]): Array[Byte] =
    writer { enc.encode(value, _) }

  def writer(f: Encoder.Output => Any): Array[Byte] =
    PooledByteArrayOutputStream.withPooled { arr =>
      val out = new Encoder.DefaultOutput(new DataOutputStream(arr))
      f(out)
      arr.toByteArray
    }

  def encode[A](buf: ByteBuffer, value: A)(implicit
      enc: Encoder[A]
  ): ByteBuffer =
    writer(buf) { enc.encode(value, _) }

  def writer(buf: ByteBuffer)(f: Encoder.Output => Any): ByteBuffer = {
    val out = new ByteBufferOutput(buf)
    f(out)
    buf
  }

  def decode(bytes: Array[Byte]): Decoder.API =
    new Decoder.API(reader(bytes))

  def reader(bytes: Array[Byte]): Decoder.Input = {
    val in = new ByteArrayInputStream(bytes)
    new Decoder.DefaultInput(new DataInputStream(in))
  }

  def decode(buf: ByteBuffer): Decoder.API =
    new Decoder.API(reader(buf))

  def reader(buf: ByteBuffer): Decoder.Input =
    new Decoder.ByteBufferInput(buf)
}

private object PooledByteArrayOutputStream {
  val AllocSize = 4 * 1024 // 4kb

  private[this] val pool =
    new Recycler[PooledByteArrayOutputStream]() {
      def newObject(
          handle: Recycler.Handle[PooledByteArrayOutputStream]
      ): PooledByteArrayOutputStream =
        new PooledByteArrayOutputStream(handle)
    }

  def withPooled[A](fn: ByteArrayOutputStream => A): A = {
    val holder = pool.get()
    try fn(holder.byteArrayOutputStream)
    finally holder.recycle()
  }
}

private final class PooledByteArrayOutputStream private (
    private[this] val handler: Recycler.Handle[PooledByteArrayOutputStream]
) {
  import PooledByteArrayOutputStream._

  val byteArrayOutputStream =
    new ByteArrayOutputStream(AllocSize)

  def recycle(): Unit = {
    byteArrayOutputStream.reset()
    handler.recycle(this)
  }
}
