package smarttrie.app.server

import bftsmart.tom.ServiceReplica

object ServerApp extends App {
  val server = new KeyValueServer(State.hashMap)
  new ServiceReplica(args(0).toInt, server, server)
}
