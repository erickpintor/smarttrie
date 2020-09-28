package smarttrie.app.client

import bftsmart.tom.ServiceProxy
import smarttrie.atoms._
import smarttrie.io._
import smarttrie.lang._

object ClientApp extends App {
  import Command._

  val proxy = new ServiceProxy(2)
  val set = Set(Key("foo".toBuf), Value("bar".toBuf))
  val reply0 = proxy.invokeOrdered(Codec.encode(set).toByteArray)
  println("------------------------------------------")
  println(Codec.decode(reply0).as[Reply])

  val get = Get(Key("foo".toBuf))
  val reply1 = proxy.invokeOrdered(Codec.encode(get).toByteArray)
  println("------------------------------------------")
  println(Codec.decode(reply1).as[Reply])
}
