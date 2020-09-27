package smarttrie.app.traditional

import bftsmart.tom.MessageContext
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable
import java.nio.ByteBuffer
import java.util
import java.util.logging.{Level, Logger}
import smarttrie.atoms._
import smarttrie.io._

final class KeyValueServer(
    private[this] var state: util.TreeMap[Key, Value] = new util.TreeMap()
) extends DefaultSingleRecoverable {
  import Command._
  import Reply._

  private[this] val logger = Logger.getLogger("server")

  def appExecuteUnordered(bytes: Array[Byte], ctx: MessageContext): Array[Byte] =
    appExecuteOrdered(bytes, ctx)

  def appExecuteOrdered(bytes: Array[Byte], ctx: MessageContext): Array[Byte] = {
    def runCommand(cmd: Command): Reply = {
      val response = cmd match {
        case Set(k, v) => Option(state.put(k, v))
        case Get(k)    => Option(state.get(k))
        case Remove(k) => Option(state.remove(k))
      }
      response.map(Data).getOrElse(NotFound)
    }

    val command = Codec.decode(bytes).tryAs[Command]
    val reply = command.fold(logAndReturn("decoding command", Error), runCommand)
    Codec.encode(reply).array()
  }

  def getSnapshot: Array[Byte] = {
    var size = 0
    state.forEach((k, v) => {
      size += Codec.size(KeyValue(k, v))
    })

    val buf = ByteBuffer.allocate(size)
    state.forEach((k, v) => {
      Codec.encode(KeyValue(k, v), buf)
    })
    buf.flip().array()
  }

  def installSnapshot(bytes: Array[Byte]): Unit = {
    state = new util.TreeMap()
    val buf = ByteBuffer.wrap(bytes)
    while (buf.hasRemaining) {
      val kv = Codec.decode(buf).as[KeyValue]
      state.put(kv.key, kv.value)
    }
  }

  private def logAndReturn[A](msg: => String, value: => A)(err: Throwable): A = {
    logger.log(Level.SEVERE, s"Error while $msg", err)
    value
  }
}
