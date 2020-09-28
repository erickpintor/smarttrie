package smarttrie.app

import bftsmart.tom.MessageContext
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable
import io.netty.buffer.Unpooled
import java.util.logging.{Level, Logger}
import scala.util.Try
import smarttrie.atoms._
import smarttrie.io._
import smarttrie.lang._

final class KeyValueServer(private[this] var state: State)
    extends DefaultSingleRecoverable {
  import Command._
  import Reply._

  private[this] val logger = Logger.getLogger("server")

  def appExecuteUnordered(bytes: Array[Byte], ctx: MessageContext): Array[Byte] =
    appExecuteOrdered(bytes, ctx)

  def appExecuteOrdered(bytes: Array[Byte], ctx: MessageContext): Array[Byte] = {
    def runCommand(cmd: Command): Reply = {
      val response = cmd match {
        case Set(k, v) => state.put(k, v)
        case Get(k)    => state.get(k)
        case Remove(k) => state.remove(k)
      }
      response.map(Data).getOrElse(NotFound)
    }

    val command = Codec.decode(bytes).tryAs[Command]
    val reply = command.fold(logAndReturn("decoding command", Error), runCommand)
    Codec.encode(reply).toByteArray
  }

  def getSnapshot: Array[Byte] =
    Codec.gathering { out =>
      state foreach {
        case (k, v) =>
          out.write(k).write(v)
      }
    }.toByteArray

  def installSnapshot(bytes: Array[Byte]): Unit = {
    def install(): Try[Unit] =
      Try {
        val input = Unpooled.wrappedBuffer(bytes)
        while (input.readableBytes() > 0) {
          val key = Codec.decode(input).as[Key]
          val value = Codec.decode(input).as[Value]
          state.put(key, value)
        }
      }

    state.clear()
    install().fold(logAndReturn("installing snapshot", ()), identity)
  }

  private def logAndReturn[A](msg: => String, value: => A)(err: Throwable): A = {
    logger.log(Level.SEVERE, s"Error while $msg", err)
    value
  }
}
