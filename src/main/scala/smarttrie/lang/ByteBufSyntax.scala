package smarttrie.lang

import java.nio.ByteBuffer

trait ByteBufferSyntax {

  implicit final class BufferToString(buf: ByteBuffer) {
    def toUTF8String: String = {
      val arr = new Array[Byte](buf.duplicate().remaining())
      buf.get(arr)
      new String(arr, UTF8)
    }
  }
}
