package benchmarks

import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.ThreadLocalRandom
import org.openjdk.jmh.annotations.{Benchmark, Scope, Setup, State, Threads}
import smarttrie.app.server._

object LogReadBench {

  val LogEntriesToAdd = 1_000_000
  val logDir = Paths.get("files")

  @State(Scope.Benchmark)
  class LogReaderState {
    var log: Log = _
    @Setup
    def setup(): Unit = {
      cleanupLogDir()
      log = Log(logDir)
      for (cid <- 0 until LogEntriesToAdd) {
        log.append(LogEntry(CID(cid), newBatch()))
      }
    }
  }

  def cleanupLogDir(): Unit =
    Files.walkFileTree(
      logDir,
      new SimpleFileVisitor[Path] {
        override def visitFile(
            file: Path,
            attrs: BasicFileAttributes
        ): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }
      }
    )

  private def newBatch(): Batch = {
    val random = ThreadLocalRandom.current()
    val batch = new Batch(10)
    for (i <- batch.indices) {
      val request = new Request(32)
      random.nextBytes(request)
      batch(i) = request
    }
    batch
  }
}

// [info] Benchmark               Mode  Cnt  Score   Error  Units
// [info] LogReadBench.readLogs  thrpt   25  1.196 ± 0.017  ops/s
class LogReadBench extends {
  import LogReadBench._

  @Benchmark
  @Threads(1)
  def readLogs(state: LogReaderState): Seq[LogEntry] = {
    import state._
    log.entries(CID(0)).toSeq
  }
}