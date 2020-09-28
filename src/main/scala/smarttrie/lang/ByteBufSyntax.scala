package smarttrie.lang

import io.netty.buffer.ByteBuf

trait ByteBufSyntax {

  implicit final class BufferToString(buf: ByteBuf) {
    def toUTF8String: String =
      buf.toString(UTF8)
  }

  implicit final class BufferToArray(buf: ByteBuf) {
    def toByteArray: Array[Byte] = {
      val len = buf.readableBytes()
      val arr = new Array[Byte](len)
      buf.readBytes(arr)
      arr
    }
  }
}
