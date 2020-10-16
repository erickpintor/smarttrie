package smarttrie.app.server

import java.nio.file.{Files, Path}
import scala.util.Random
import smarttrie.atoms._
import smarttrie.io._
import smarttrie.test._

abstract class LogSpec(name: String, sync: Boolean) extends Spec {

  var logDir: Path = _
  var log: Log = _

  before {
    logDir = Files.createTempDirectory("smarttrie-log")
    log = Log(logDir, sync)
  }

  after {
    if (log ne null) {
      log.close()
    }
    if (logDir ne null) {
      IO.cleanDirectory(logDir)
    }
  }

  name should "append entries" in {
    log.close()
    log = Log(logDir, sync, logFileSizeMB = 1)
    val batches = Seq.newBuilder[LogEntry]

    for (id <- 0 until 10_000) {
      val cid = CID(id)
      val batch = (0 until 10).map(_ => Random.nextBytes(32)).toArray
      val entry = LogEntry(cid, batch)
      log.append(entry)
      batches += entry
    }

    val entries = log.entries(from = CID(0)).toSeq
    entries should contain theSameElementsInOrderAs batches.result()
  }

  it should "list entries with cursor" in {
    for (id <- 0 until 10) {
      log.append(LogEntry(CID(id), new Batch(0)))
    }

    val entries = log.entries(from = CID(5)).toSeq
    all(entries map (_.cid.toInt)) should be >= 5
  }

  it should "be durable" in {
    val entry = LogEntry(CID(0), new Batch(0))
    log.append(entry)
    log.close()

    log = Log(logDir, sync)
    log.entries(from = CID(0)).toSeq should contain only entry
  }

  it should "truncate" in {
    (0 to 5) foreach (id => log.append(LogEntry(CID(id), new Batch(0))))
    log.close() // force flush

    log = Log(logDir, sync)
    (6 to 10) foreach (id => log.append(LogEntry(CID(id), new Batch(0))))
    log.close() // force flush

    log.truncate(to = CID(5))
    val entries = log.entries(from = CID(0)).toSeq
    all(entries map (_.cid.toInt)) should be > 5
  }
}

class SyncLog extends LogSpec("SyncLog", sync = true)
class AsyncLog extends LogSpec("AsyncLog", sync = false)
