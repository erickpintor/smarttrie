package smarttrie.lang
import java.nio.ByteBuffer

trait StringSyntax {

  implicit final class StringToByteBuffer(value: String) {
    def toBuf: ByteBuffer =
      ByteBuffer
        .wrap(value.getBytes(UTF8))
        .asReadOnlyBuffer()
  }
}
