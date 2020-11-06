package smarttrie.app.server

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.{BufferOverflowException, BufferUnderflowException, MappedByteBuffer}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}
import org.slf4j.{Logger, LoggerFactory}
import scala.annotation.tailrec
import smarttrie.atoms._
import smarttrie.io._

trait CheckpointController {
  val ckpPath: Path
  val blockSizeMB: Int

  def write(cid: CID, state: State): Unit

  def shutdown(): Unit

  final def read(): Option[CheckpointReader] =
    Checkpoint.read(ckpPath, blockSizeMB)
}

object CheckpointController {

  def apply(
      ckpPath: Path,
      async: Boolean,
      blockSizeMB: Int = Checkpoint.DefaultBlockSizeMB
  ): CheckpointController =
    if (async) new Async(ckpPath, blockSizeMB)
    else new Sync(ckpPath, blockSizeMB)

  final class Async(val ckpPath: Path, val blockSizeMB: Int)
      extends CheckpointController {

    private[this] val logger = LoggerFactory.getLogger(getClass)
    private[this] val isCheckpointing = new AtomicBoolean(false)
    private[this] val ckpThread =
      Executors.newSingleThreadExecutor(r => new Thread(r, "Checkpoint-Thread"))

    def write(cid: CID, state: State): Unit =
      if (isCheckpointing.compareAndSet(false, true)) {
        ckpThread.execute(() => {
          withLogging(logger, cid) {
            Checkpoint.write(ckpPath, cid, state, blockSizeMB)
          }
          isCheckpointing.set(false)
        })
      } else {
        logger.warn("Skipping a checkpoint at {}", cid)
      }

    def shutdown(): Unit = {
      ckpThread.shutdown()
      ckpThread.awaitTermination(1, TimeUnit.HOURS) // long enough.
    }
  }

  final class Sync(val ckpPath: Path, val blockSizeMB: Int)
      extends CheckpointController {

    private[this] val logger = LoggerFactory.getLogger(getClass)

    def write(cid: CID, state: State): Unit =
      withLogging(logger, cid) {
        Checkpoint.write(ckpPath, cid, state, blockSizeMB)
      }

    def shutdown(): Unit = ()
  }

  @inline
  private def withLogging(logger: Logger, cid: CID)(fn: => Any): Unit = {
    val start = System.currentTimeMillis()
    logger.info("Starting replica checkpoint at {}", cid)
    fn

    val delta = System.currentTimeMillis() - start
    logger.info("Checkpoint created after {}s", delta / 1000)
  }
}

object Checkpoint {

  val DefaultBlockSizeMB = 512
  val CheckpointExtension = ".ckp"
  val TempExtension = ".ckp.tmp"

  def write(ckpPath: Path, cid: CID, state: State, blockSizeMB: Int = 512): Unit = {
    val blockSize = blockSizeMB * 1024 * 1024
    val writer = new CheckpointAsyncWriter(ckpPath, blockSize)
    writer.write(cid, state)
  }

  def read(ckpPath: Path, blockSizeMB: Int = 512): Option[CheckpointReader] = {
    val blockSize = blockSizeMB * 1024 * 1024
    val checkpoints = IO.listFiles(ckpPath, CheckpointExtension)
    require(checkpoints.size <= 1, "Multiple checkpoints found")
    checkpoints.headOption map { new CheckpointReader(_, blockSize) }
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

  @tailrec
  private def append(key: Key, value: Value): Unit =
    try {
      val pos = buffer.position()
      Codec.encode(buffer, key)
      Codec.encode(buffer, value)
      bytesWritten += buffer.position() - pos
    } catch {
      case _: BufferOverflowException =>
        buffer = channel.map(READ_WRITE, bytesWritten, blockSize)
        append(key, value)
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

final class CheckpointReader(filePath: Path, blockSize: Int) {
  import FileChannel.MapMode._
  import StandardOpenOption._

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] var channel = FileChannel.open(filePath, READ)
  private[this] var buffer =
    channel.map(READ_ONLY, 0, math.min(channel.size(), blockSize))

  val lastCID: CID =
    Codec.decode(buffer).as[CID]

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
              val nextChunk = math.min(channel.size() - pos, blockSize)
              buffer = channel.map(READ_ONLY, pos, nextChunk)
              if (buffer.hasRemaining) {
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
