package smarttrie.atoms

import java.nio.ByteBuffer

final case class Key(internalBuffer: ByteBuffer) extends AnyVal {
  @inline def buf: ByteBuffer = internalBuffer.duplicate().asReadOnlyBuffer()
  @inline def size: Int = internalBuffer.remaining()
}

final case class Value(internalBuffer: ByteBuffer) extends AnyVal {
  @inline def buf: ByteBuffer = internalBuffer.duplicate().asReadOnlyBuffer()
  @inline def size: Int = internalBuffer.remaining()
}
