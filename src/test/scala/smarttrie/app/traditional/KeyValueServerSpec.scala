package smarttrie.app.traditional

import bftsmart.tom.MessageContext
import java.util
import smarttrie.atoms._
import smarttrie.io._
import smarttrie.lang._
import smarttrie.test._

class KeyValueServerSpec extends Spec {
  import Command._
  import Reply._

  val aKey = Key("foo".toBuf)
  val bKey = Key("fuzz".toBuf)
  val aValue = Value("bar".toBuf)
  val bValue = Value("bax".toBuf)
  val nullCtx = new MessageContext(0, 0, null, 0, 0, 0, 0, null, 0, 0, 0, 0, 0, 0,
    null, null, false)

  "KeyValueServer" should "add a key" in {
    val server = new KeyValueServer()
    run(server, Set(aKey, aValue)) shouldBe NotFound
    run(server, Set(aKey, bValue)) shouldBe Data(aValue) // returns old value
  }

  it should "read a key" in {
    val server = new KeyValueServer({
      val state = new util.TreeMap[Key, Value]()
      state.put(aKey, aValue)
      state
    })
    run(server, Get(aKey)) shouldBe Data(aValue)
  }

  it should "remove a key" in {
    val server = new KeyValueServer({
      val state = new util.TreeMap[Key, Value]()
      state.put(aKey, aValue)
      state
    })
    run(server, Remove(aKey)) shouldBe Data(aValue) // return deleted value
    run(server, Remove(aKey)) shouldBe NotFound
  }

  it should "take/restore snapshot" in {
    val server = new KeyValueServer({
      val state = new util.TreeMap[Key, Value]()
      state.put(aKey, aValue)
      state.put(bKey, bValue)
      state
    })

    val snapshot = server.getSnapshot
    run(server, Remove(aKey)) shouldBe Data(aValue)
    run(server, Get(aKey)) shouldBe NotFound

    server.installSnapshot(snapshot)
    run(server, Get(aKey)) shouldBe Data(aValue)
  }

  private def run(server: KeyValueServer, cmd: Command): Reply = {
    val response = server.appExecuteOrdered(Codec.encode(cmd).toByteArray, nullCtx)
    Codec.decode(response).as[Reply]
  }
}