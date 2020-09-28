package smarttrie.atoms

import smarttrie.test._
//import smarttrie.lang._

class ReplySpec extends Spec with CodecAsserts {
  import Reply._

  "Reply" should "encode/decode" in {
    verifyCodec(Error: Reply)
//    verifyCodec(NotFound: Reply)
//    verifyCodec(Data(Value("foo".toBuf)): Reply)
  }
}
