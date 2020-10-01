package smarttrie.app.server

import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable
import bftsmart.tom.{MessageContext, ServiceReplica}
import org.slf4j.LoggerFactory
import scala.util.control.NonFatal
import smarttrie.atoms._
import smarttrie.io._

object Server extends App {
  val server = new Server(State.hashMap)
  new ServiceReplica(args(0).toInt, server, server)
}

final class Server(private[this] var state: State) extends DefaultSingleRecoverable {
  import Command._
  import Reply._

  private[this] val logger = LoggerFactory.getLogger("server")

  def appExecuteUnordered(bytes: Array[Byte], ctx: MessageContext): Array[Byte] =
    appExecuteOrdered(bytes, ctx)

  def appExecuteOrdered(bytes: Array[Byte], ctx: MessageContext): Array[Byte] = {
    def execute(bytes: Array[Byte]): Array[Byte] = {
      val cmd = Codec.decode(bytes).as[Command]
      val res = runCommand(cmd)
      Codec.encode(res)
    }

    def runCommand(cmd: Command): Reply = {
      val res = cmd match {
        case Get(k)    => state.get(k)
        case Set(k, v) => state.put(k, v)
        case Remove(k) => state.remove(k)
      }
      res.map(Data).getOrElse(Null)
    }

    try execute(bytes)
    catch {
      case NonFatal(err) =>
        logger.error("Error while executing command", err)
        Codec.encode(Error)
    }
  }

  def getSnapshot: Array[Byte] =
    Codec.writer { out =>
      state foreach {
        case (key, value) =>
          out
            .write(key)
            .write(value)
      }
    }

  def installSnapshot(bytes: Array[Byte]): Unit =
    try {
      state.clear()
      val in = Codec.reader(bytes)
      while (in.hasMoreBytes) {
        val key = in.read[Key]
        val value = in.read[Value]
        state.put(key, value)
      }
    } catch {
      case NonFatal(err) =>
        logger.error("Error while installing a snapshot", err)
    }
}
