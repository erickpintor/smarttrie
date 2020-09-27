package smarttrie.lang
import java.nio.ByteBuffer
import java.nio.charset.Charset

trait StringSyntax {

  private[this] val UTF8 = Charset.forName("UTF-8")

  implicit final class StringToByteBuffer(value: String) {
    def toBuf: ByteBuffer = ByteBuffer.wrap(value.getBytes(UTF8)).asReadOnlyBuffer()
  }
}
