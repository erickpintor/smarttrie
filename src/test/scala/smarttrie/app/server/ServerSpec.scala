package smarttrie.app.server

import bftsmart.tom.MessageContext
import smarttrie.atoms._
import smarttrie.io._
import smarttrie.lang._
import smarttrie.test._

class ServerSpec extends Spec {
  import Command._
  import Reply._

  check("State.hashMap", State.hashMap)
  check("State.trieMap", State.trieMap)

  def check(name: String, newState: => State): Unit = {
    def aKey = Key("foo".toUTF8Array)
    def bKey = Key("fuzz".toUTF8Array)
    def aValue = Value("bar".toUTF8Array)
    def bValue = Value("bax".toUTF8Array)
    def nullCtx =
      new MessageContext(0, 0, null, 0, 0, 0, 0, null, 0, 0, 0, 0, 0, 0, null, null,
        false)

    s"Server($name)" should "add a key" in {
      val server = new Server(newState)
      run(server, Set(aKey, aValue)) shouldBe Null
      run(server, Set(aKey, bValue)) shouldBe Data(aValue) // returns old value
    }

    it should "read a key" in {
      val server = new Server({
        val state = newState
        state.put(aKey, aValue)
        state
      })
      run(server, Get(aKey)) shouldBe Data(aValue)
    }

    it should "remove a key" in {
      val server = new Server({
        val state = newState
        state.put(aKey, aValue)
        state
      })
      run(server, Remove(aKey)) shouldBe Data(aValue) // return deleted value
      run(server, Remove(aKey)) shouldBe Null
    }

    it should "take/restore snapshot" in {
      val server = new Server({
        val state = newState
        state.put(aKey, aValue)
        state.put(bKey, bValue)
        state
      })

      val snapshot = server.getSnapshot
      run(server, Remove(aKey)) shouldBe Data(aValue)
      run(server, Get(aKey)) shouldBe Null

      server.installSnapshot(snapshot)
      run(server, Get(aKey)) shouldBe Data(aValue)
    }

    def run(server: Server, cmd: Command): Reply = {
      val response = server.appExecuteOrdered(Codec.encode(cmd), nullCtx)
      Codec.decode(response).as[Reply]
    }
  }
}
