package smarttrie

import java.nio.charset.Charset

package object lang extends StringSyntax with ByteBufferSyntax {
  final val UTF8 = Charset.forName("UTF-8")
}
