package smarttrie.atoms

import smarttrie.io._
import smarttrie.lang._
import smarttrie.test._

class CommandSpec extends Spec {
  import Command._

  "Command" should "serialize/deserialize" in {
    def check[A: Codec](command: A): Unit = {
      val encoded = Codec.encode(command)
      val decoded = Codec.decode(encoded).as[A]
      decoded shouldBe command
    }
    check(Get(Key("foo".toBuf)))
    check(Set(Key("foo".toBuf), Value("bar".toBuf)))
  }
}
