package smarttrie.io

import io.netty.util.Recycler
import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream
}
import java.nio.ByteBuffer

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

  object ByteArrayOutput {

    private[this] val pool = new Recycler[ByteArrayOutput]() {
      def newObject(handle: Recycler.Handle[ByteArrayOutput]): ByteArrayOutput =
        new ByteArrayOutput(handle)
    }

    def pooled[A](fn: ByteArrayOutput => Any): Array[Byte] = {
      val out = pool.get()
      fn(out)
      val res = out.arr.toByteArray
      out.arr.reset()
      out.out = new DataOutputStream(out.arr)
      out.handle.recycle(out)
      res
    }
  }

  final class ByteArrayOutput private (
      private val handle: Recycler.Handle[ByteArrayOutput]
  ) extends Output {

    private val arr = new ByteArrayOutputStream(4 * 1024) // 4kb
    private var out = new DataOutputStream(arr)

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

  object ByteBufferOutput {

    private[this] val pool = new Recycler[ByteBufferOutput]() {
      def newObject(handle: Recycler.Handle[ByteBufferOutput]): ByteBufferOutput =
        new ByteBufferOutput(handle)
    }

    def pooled(buf: ByteBuffer)(f: ByteBufferOutput => Any): ByteBuffer = {
      val out = pool.get()
      out.out = buf
      f(out)
      out.out = null
      out.handle.recycle(out)
      buf
    }
  }

  final class ByteBufferOutput private (
      private val handle: Recycler.Handle[ByteBufferOutput]
  ) extends Output {

    private var out: ByteBuffer = _

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

  object SizeCountOutput {

    private[this] val pool = new Recycler[SizeCountOutput]() {
      def newObject(handle: Recycler.Handle[SizeCountOutput]): SizeCountOutput =
        new SizeCountOutput(handle)
    }

    def pooled(fn: SizeCountOutput => Any): Long = {
      val out = pool.get()
      fn(out)
      val res = out.count
      out.count = 0
      out.handle.recycle(out)
      res
    }
  }

  final class SizeCountOutput(private val handle: Recycler.Handle[SizeCountOutput])
      extends Output {

    private var count = 0L

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

  def sizeOf[A](value: A)(implicit enc: Encoder[A]): Long =
    Encoder.SizeCountOutput.pooled(enc.encode(value, _))

  def encode[A](value: A)(implicit enc: Encoder[A]): Array[Byte] =
    writer { enc.encode(value, _) }

  def writer(f: Encoder.Output => Any): Array[Byte] =
    Encoder.ByteArrayOutput.pooled(f)

  def encode[A](buf: ByteBuffer, value: A)(implicit
      enc: Encoder[A]
  ): ByteBuffer =
    writer(buf) { enc.encode(value, _) }

  def writer(buf: ByteBuffer)(f: Encoder.Output => Any): ByteBuffer =
    Encoder.ByteBufferOutput.pooled(buf)(f)

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
