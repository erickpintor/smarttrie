package smarttrie.lang.syntax

trait ByteArraySyntax {

  implicit final class ByteArraySize(bytes: Array[Byte]) {
    def lengthInMB: Int = bytes.length / 1024 / 1024
  }
}
