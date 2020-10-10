package smarttrie.test

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import smarttrie.io._

trait Spec extends AnyFlatSpec with BeforeAndAfter with Matchers

trait CodecAsserts { self: Spec =>
  def verifyCodec[A: Codec](value: A): Unit = {
    val encoded = Codec.encode(value)
    val decoded = Codec.decode(encoded).as[A]
    decoded shouldBe value
  }
}
