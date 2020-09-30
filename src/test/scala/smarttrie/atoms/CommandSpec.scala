package smarttrie.atoms

import smarttrie.lang._
import smarttrie.test._

class CommandSpec extends Spec with CodecAsserts {
  import Command._

  "Command" should "serialize/deserialize" in {
    verifyCodec(Get(Key("foo".toUTF8Array)): Command)
    verifyCodec(Remove(Key("foo".toUTF8Array)): Command)
    verifyCodec(Set(Key("foo".toUTF8Array), Value("bar".toUTF8Array)): Command)
  }
}
