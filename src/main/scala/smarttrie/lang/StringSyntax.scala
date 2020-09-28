package smarttrie.lang

import io.netty.buffer.{ByteBuf, Unpooled}
import java.nio.charset.Charset

trait StringSyntax {

  final val UTF8 = Charset.forName("UTF-8")

  implicit final class StringToByteBuffer(value: String) {
    def toBuf: ByteBuf =
      Unpooled
        .wrappedBuffer(value.getBytes(UTF8))
        .asReadOnly()
  }
}
