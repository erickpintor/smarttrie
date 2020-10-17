package smarttrie.app.server

import java.io.Closeable
import java.nio.channels.FileChannel
import java.nio.file._
import java.nio.{
  BufferOverflowException,
  BufferUnderflowException,
  ByteBuffer,
  MappedByteBuffer
}
import java.util
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.util.Random
import smarttrie.atoms._
import smarttrie.io._

final case class LogEntry(cid: CID, batch: Batch) {

  override def hashCode(): Int =
    batch.foldLeft(batch.hashCode()) {
      case (acc, request) =>
        acc * util.Arrays.hashCode(request)
    }

  override def equals(obj: Any): Boolean =
    obj match {
      case LogEntry(otherCID, otherBatch) =>
        cid == otherCID &&
        batch.length == otherBatch.length
        batch.zip(otherBatch) forall {
          case (reqA, reqB) =>
            util.Arrays.equals(reqA, reqB)
        }
      case _ => false
    }
}

object LogEntry {

  implicit object LogEntryCodec extends Codec[LogEntry] {

    def encode(entry: LogEntry, out: Encoder.Output): Unit = {
      out
        .write(entry.cid)
        .writeInt(entry.batch.length)

      entry.batch foreach { request =>
        out
          .writeInt(request.length)
          .writeBytes(request)
      }
    }

    def decode(in: Decoder.Input): LogEntry = {
      val cid = in.read[CID]
      val batchSize = in.readInt
      val batch = new Batch(batchSize)

      for (i <- 0 until batchSize) {
        val requestSize = in.readInt
        val request = in.readBytes(requestSize)
        batch(i) = request
      }

      LogEntry(cid, batch)
    }
  }
}

object Log {

  val LogExtension = ".log"
  val TempExtension = ".log.tmp"

  private val logger = LoggerFactory.getLogger(getClass)

  def apply(logDir: Path, sync: Boolean = false, logFileSizeMB: Int = 512): Log =
    new Log(logDir.toAbsolutePath, sync, logFileSizeMB)
}

final class Log private (logDir: Path, sync: Boolean, logFileSizeMB: Int) {
  import Log._

  private[this] var log = newLogFile

  private def newLogFile: LogWriter =
    LogWriter(
      logDir.resolve(s"${Random.nextLong().abs}$TempExtension"),
      logFileSizeMB,
      sync
    )

  def append(entry: LogEntry): Unit =
    synchronized {
      while (!log.append(entry)) {
        flush()
      }
    }

  private def flush(): Unit =
    synchronized {
      if (log.size > 0) {
        val name = f"${log.start.toInt}%010d-${log.end.toInt}%010d$LogExtension"
        val path = logDir.resolve(name)
        logger.info(s"Flushing log file $path")
        log.close()
        log.rename(path)
        log = newLogFile
      }
    }

  def truncate(to: CID): Unit = {
    logger.info(s"Truncating log files up to $to")
    IO.listFiles(logDir, LogExtension)
      .view
      .map(LogReader(_))
      .sortBy(_.end)
      .takeWhile(_.end <= to)
      .foreach(_.delete())
  }

  def close(): Unit =
    flush()

  def entries(from: CID): Iterator[LogEntry] with Closeable = {
    flush()
    new MergeIterator(from)
  }

  private final class MergeIterator(from: CID)
      extends Iterator[LogEntry]
      with Closeable {

    private[this] var lastSeenCID = Option.empty[CID]
    private[this] var iterator: Iterator[LogEntry] with Closeable =
      new LogFilesIterator(from)

    @tailrec
    def hasNext: Boolean =
      if (iterator.hasNext) {
        true
      } else {
        lastSeenCID match {
          case Some(cid) =>
            iterator = entries(cid.next)
            lastSeenCID = None
            hasNext

          case None =>
            close()
            false
        }
      }

    def next(): LogEntry = {
      val res = iterator.next()
      lastSeenCID = Some(res.cid)
      res
    }

    def close(): Unit =
      iterator.close()
  }

  private final class LogFilesIterator(from: CID)
      extends Iterator[LogEntry]
      with Closeable {

    private[this] var iterator: Iterator[LogEntry] = _

    private[this] var filesToRead =
      IO.listFiles(logDir, LogExtension)
        .view
        .map(LogReader(_))
        .sortBy(_.start)
        .dropWhile(_.end < from)

    @tailrec
    def hasNext: Boolean =
      if (filesToRead.isEmpty) {
        false
      } else {
        if (iterator eq null) {
          iterator = filesToRead.head.entries(from)
          hasNext
        } else {
          if (iterator.hasNext) {
            true
          } else {
            filesToRead.head.close()
            filesToRead = filesToRead.tail
            iterator = null
            hasNext
          }
        }
      }

    def next(): LogEntry =
      iterator.next()

    def close(): Unit =
      filesToRead foreach (_.close())
  }
}

final case class LogFileMetadata(
    var maxSizeBytes: Long,
    var start: CID,
    var end: CID
)

object LogFileMetadata {

  implicit object LogFileMetadataCodec extends Codec[LogFileMetadata] {

    def encode(metadata: LogFileMetadata, out: Encoder.Output): Unit =
      out
        .writeLong(metadata.maxSizeBytes)
        .write(metadata.start)
        .write(metadata.end)

    def decode(in: Decoder.Input): LogFileMetadata =
      LogFileMetadata(
        in.readLong,
        in.read[CID],
        in.read[CID]
      )
  }

  val MetadataByteSize =
    Codec.sizeOf(LogFileMetadata(0L, CID.Null, CID.Null)).toInt
}

trait LogWriter {
  def append(entry: LogEntry): Boolean
  def rename(newFilePath: Path): Unit
  def start: CID
  def end: CID
  def size: Long
  def close(): Unit
}

object LogWriter {
  def apply(filePath: Path, maxSizeMB: Int, sync: Boolean): LogWriter = {
    val maxSize = maxSizeMB * 1024 * 1024L
    if (sync) new SyncLogWriter(filePath, maxSize)
    else new AsyncLogWriter(filePath, maxSize)
  }
}

final class SyncLogWriter(filePath: Path, maxSize: Long) extends LogWriter {
  import LogFileMetadata._
  import StandardOpenOption._

  private[this] val channel = FileChannel.open(filePath, CREATE_NEW, WRITE, DSYNC)
  private[this] var buffer = ByteBuffer.allocateDirect(4 * 1024) // 4KB
  private[this] val meta = LogFileMetadata(0, CID.Null, CID.Null)
  private[this] var _size = 0L

  Codec.encode(buffer, meta)
  channel.write(buffer.flip())
  buffer.clear()

  def start: CID = meta.start
  def end: CID = meta.end
  def size: Long = _size

  def append(entry: LogEntry): Boolean = {
    try {
      Codec.encode(buffer, entry)
    } catch {
      case _: BufferOverflowException => // buffer too small
        buffer.clear()
        buffer = ByteBuffer.allocateDirect(Codec.sizeOf(entry).toInt)
        Codec.encode(buffer, entry)
    }

    val entrySize = buffer.flip().limit()

    if (_size + entrySize > maxSize) {
      buffer.clear()
      false

    } else {
      channel.write(buffer)
      _size += entrySize
      buffer.clear()

      if (meta.start == CID.Null) {
        meta.start = entry.cid
      }
      meta.end = entry.cid
      meta.maxSizeBytes = _size

      true
    }
  }

  def rename(newFilePath: Path): Unit = {
    close()
    Files.move(filePath, newFilePath)
  }

  def close(): Unit = {
    if (channel.isOpen) {
      Codec.encode(buffer, meta)
      channel.position(0)
      channel.write(buffer.flip())
      buffer.clear()
    }
    channel.close()
  }
}

final class AsyncLogWriter(filePath: Path, maxSize: Long) extends LogWriter {
  import FileChannel.MapMode._
  import LogFileMetadata._
  import StandardOpenOption._

  private[this] val channel = FileChannel.open(filePath, CREATE_NEW, WRITE, READ)
  private[this] val buffer = channel.map(READ_WRITE, 0, MetadataByteSize + maxSize)
  private[this] val meta = LogFileMetadata(0, CID.Null, CID.Null)
  private[this] var _size = 0L
  buffer.position(MetadataByteSize)

  def start: CID = meta.start
  def end: CID = meta.end
  def size: Long = _size

  def append(entry: LogEntry): Boolean = {
    val entrySize = Codec.sizeOf(entry)

    if (_size + entrySize > maxSize) {
      false

    } else {
      Codec.encode(buffer, entry)
      _size += entrySize

      if (meta.start == CID.Null) {
        meta.start = entry.cid
      }
      meta.end = entry.cid
      meta.maxSizeBytes = _size

      true
    }
  }

  def rename(newFilePath: Path): Unit = {
    close()
    Files.move(filePath, newFilePath)
  }

  def close(): Unit = {
    if (channel.isOpen) {
      Codec.encode(buffer.rewind(), meta)
      buffer.force()
    }
    channel.close()
  }
}

object LogReader {
  import LogFileMetadata._
  import StandardOpenOption._

  def apply(filePath: Path): LogReader = {
    val ch = FileChannel.open(filePath, READ)
    val metaBuf = ByteBuffer.allocate(MetadataByteSize)
    ch.read(metaBuf)
    metaBuf.flip()

    val meta = Codec.decode(metaBuf).as[LogFileMetadata]
    new LogReader(filePath, ch, meta)
  }
}

final case class LogReader private (
    filePath: Path,
    channel: FileChannel,
    meta: LogFileMetadata
) {
  import FileChannel.MapMode._
  import LogFileMetadata._

  private[this] var mBuffer: MappedByteBuffer = _

  def start: CID = meta.start
  def end: CID = meta.end
  def size: Long = meta.maxSizeBytes

  def entries(from: CID): Iterator[LogEntry] = {
    mBuffer = channel.map(READ_ONLY, MetadataByteSize, size)

    new Iterator[LogEntry] {

      private[this] var _next: LogEntry = _

      def hasNext: Boolean =
        if (_next eq null) {
          try {
            do {
              _next = Codec.decode(mBuffer).as[LogEntry]
            } while (_next.cid < from)
            true
          } catch {
            case _: BufferUnderflowException =>
              close()
              false
          }
        } else {
          true
        }

      def next(): LogEntry = {
        val res = _next
        _next = null
        res
      }
    }
  }

  def close(): Unit = {
    mBuffer = null
    channel.close()
  }

  def delete(): Unit = {
    close()
    Files.delete(filePath)
  }
}
