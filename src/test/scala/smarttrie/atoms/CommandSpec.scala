package smarttrie.atoms

import smarttrie.lang._
import smarttrie.test._

class CommandSpec extends Spec with CodecAsserts {
  import Command._

  "Command" should "serialize/deserialize" in {
    verifyCodec(Get(Key("foo".toBuf)): Command)
    verifyCodec(Remove(Key("foo".toBuf)): Command)
    verifyCodec(Set(Key("foo".toBuf), Value("bar".toBuf)): Command)
  }
}
