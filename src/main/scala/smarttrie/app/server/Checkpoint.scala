package smarttrie.app.server

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.{BufferUnderflowException, ByteBuffer, MappedByteBuffer}
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import smarttrie.atoms._
import smarttrie.io._

object Checkpoint {

  val CheckpointExtension = ".ckp"
  val TempExtension = ".ckp.tmp"

  def write(ckpPath: Path, state: State, cid: CID, blockSizeMB: Int = 512): Unit = {
    val blockSize = blockSizeMB * 1024 * 1024
    val writer = new CheckpointAsyncWriter(ckpPath, blockSize)
    writer.write(cid, state)
  }

  def read(ckpPath: Path): Option[CheckpointReader] = {
    val checkpoints = IO.listFiles(ckpPath, CheckpointExtension)
    require(checkpoints.size <= 1, "Multiple checkpoints found")
    checkpoints.headOption map { new CheckpointReader(_) }
  }
}

final class CheckpointAsyncWriter(ckpPath: Path, blockSize: Long) {
  import Checkpoint._
  import FileChannel.MapMode._
  import StandardOpenOption._

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] var tempFile: Path = _
  private[this] var finalFile: Path = _
  private[this] var channel: FileChannel = _
  private[this] var buffer: MappedByteBuffer = _
  private[this] var bytesWritten = 0L

  def write(cid: CID, state: State): Unit = {
    require(channel eq null, "Can't reuse checkpoint writer")
    openCheckpointFile()

    Codec.encode(buffer, cid)
    bytesWritten += buffer.position()

    state foreach {
      case (key, value) =>
        append(key, value)
    }

    close()

    logger.info(s"Saved a checkpoint of ${bytesWritten / 1024 / 1024}MB")
  }

  private def openCheckpointFile(): Unit = {
    val now = System.currentTimeMillis()
    tempFile = ckpPath.resolve(s"$now$TempExtension")
    finalFile = ckpPath.resolve(s"$now$CheckpointExtension")
    channel = FileChannel.open(tempFile, CREATE_NEW, WRITE, READ)
    buffer = channel.map(READ_WRITE, 0, blockSize)
  }

  private def append(key: Key, value: Value): Unit = {
    val size = Codec.sizeOf(key) + Codec.sizeOf(value)
    if (buffer.remaining() < size) {
      buffer = channel.map(READ_WRITE, buffer.position(), blockSize)
    }
    Codec.encode(buffer, key)
    Codec.encode(buffer, value)
    bytesWritten += size
  }

  private def close(): Unit = {
    buffer.force()
    channel.truncate(bytesWritten)
    channel.close()
    buffer = null // help GC
    channel = null // help GC

    val oldCheckpoints = IO.listFiles(ckpPath, CheckpointExtension)
    Files.move(tempFile, finalFile)
    oldCheckpoints foreach Files.delete
  }
}

final class CheckpointReader(filePath: Path) {
  import StandardOpenOption._

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] var channel = FileChannel.open(filePath, READ)
  private[this] var buffer = ByteBuffer.allocateDirect(4 * 1024) // 4KB

  val lastCID: CID = {
    channel.read(buffer)
    buffer.flip()
    Codec.decode(buffer).as[CID]
  }

  def entries(): Iterator[(Key, Value)] = {
    logger.info(s"Loading a checkpoint of ${channel.size() / 1024 / 1024}MB")
    var _next: (Key, Value) = null
    var pos = 0

    new Iterator[(Key, Value)] {
      @tailrec def hasNext: Boolean =
        if (_next ne null) {
          true
        } else {
          try {
            buffer.mark()
            val key = Codec.decode(buffer).as[Key]
            val value = Codec.decode(buffer).as[Value]
            _next = (key, value)
            true
          } catch {
            case _: BufferUnderflowException =>
              buffer.reset()
              pos += buffer.position()
              buffer.clear()
              if (channel.read(buffer, pos) > 0) {
                buffer.flip()
                hasNext
              } else {
                close()
                false
              }
          }
        }

      def next(): (Key, Value) = {
        val res = _next
        _next = null
        res
      }
    }
  }

  def close(): Unit =
    if (channel ne null) {
      channel.close()
      buffer = null // help GC
      channel = null // help GC
    }

}
