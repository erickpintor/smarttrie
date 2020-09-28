package smarttrie.app.server

import bftsmart.tom.ServiceReplica
import java.nio.file.{Files, Paths}

object ServerApp extends App {
  Files.deleteIfExists(Paths.get("./config/currentView"))
  val server = new KeyValueServer(State.hashMap)
  new ServiceReplica(args(0).toInt, server, server)
}
