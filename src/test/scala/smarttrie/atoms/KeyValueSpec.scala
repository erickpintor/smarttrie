package smarttrie.atoms

import smarttrie.lang._
import smarttrie.test._

class KeyValueSpec extends Spec with CodecAsserts {

  "Key" should "encode/decode" in {
    verifyCodec(Key("foo".toBuf))
  }

  "Value" should "encode/decode" in {
    verifyCodec(Value("foo".toBuf))
  }

  "KeyValue" should "encode/decode" in {
    verifyCodec(KeyValue(Key("foo".toBuf), Value("bar".toBuf)))
  }
}
