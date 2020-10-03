package smarttrie.lang.syntax

import java.nio.charset.Charset

trait StringSyntax {

  final val UTF8 = Charset.forName("UTF-8")

  implicit final class StringToByteBuffer(value: String) {
    def toUTF8Array: Array[Byte] =
      value.getBytes(UTF8)
  }
}
