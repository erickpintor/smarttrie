package smarttrie.app.traditional

import bftsmart.tom.MessageContext
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable
//import scala.collection.concurrent.TrieMap
//import smarttrie.atoms.{Key, Value}

final class KeyValueServer extends DefaultSingleRecoverable {

//  private[this] val state = new TrieMap[Key, Value]()

  def appExecuteOrdered(
      bytes: Array[Byte],
      messageContext: MessageContext
  ): Array[Byte] = ???

  def appExecuteUnordered(
      bytes: Array[Byte],
      messageContext: MessageContext
  ): Array[Byte] = ???

  def installSnapshot(bytes: Array[Byte]): Unit = ???

  def getSnapshot: Array[Byte] = ???
}
