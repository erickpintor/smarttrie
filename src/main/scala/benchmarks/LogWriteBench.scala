package benchmarks

import bftsmart.tom.server.defaultservices.durability.DurableStateLog
import bftsmart.tom.server.defaultservices.{DiskStateLog, StateLog}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.ThreadLocalRandom
import org.openjdk.jmh.annotations.{Benchmark, Scope, Setup, State, Threads}
import smarttrie.app.server._

object LogWriteBench {

  val logDir = Paths.get("files")

  abstract class AbstractBFTStateLog {
    var cid = -1
    var log: StateLog = _
    def newLog: StateLog

    def nextID: Int = {
      cid += 1
      cid
    }

    @Setup
    def setup(): Unit = {
      cleanupLogDir()
      log = newLog
    }
  }

  @State(Scope.Benchmark)
  class SyncDiskStateLog extends AbstractBFTStateLog {
    def newLog =
      new DiskStateLog(
        0,
        new Array(0),
        new Array(0),
        true,
        true,
        false
      )
  }

  @State(Scope.Benchmark)
  class AsyncDiskStateLog extends AbstractBFTStateLog {
    def newLog =
      new DiskStateLog(
        0,
        new Array(0),
        new Array(0),
        true,
        false,
        false
      )
  }

  @State(Scope.Benchmark)
  class SyncDurableStateLog extends AbstractBFTStateLog {
    def newLog =
      new DurableStateLog(
        0,
        new Array(0),
        new Array(0),
        true,
        true,
        false
      )
  }

  @State(Scope.Benchmark)
  class AsyncDurableStateLog extends AbstractBFTStateLog {
    def newLog =
      new DurableStateLog(
        0,
        new Array(0),
        new Array(0),
        true,
        false,
        false
      )
  }

  abstract class AbstractSmartTrieLog(sync: Boolean) {
    var cid = CID.Null
    var log: Log = _

    def nextID: CID = {
      cid = cid.next
      cid
    }

    @Setup
    def setup(): Unit = {
      cleanupLogDir()
      log = Log(logDir, sync = sync)
    }
  }

  @State(Scope.Benchmark)
  class SyncSmartTrieLog extends AbstractSmartTrieLog(sync = true)

  @State(Scope.Benchmark)
  class AsyncSmartTrieLog extends AbstractSmartTrieLog(sync = false)

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

// [info] Benchmark                       Mode  Cnt       Score       Error  Units
// [info] LogWriteBench.asyncDiskStateLog     thrpt   25  103676.029 ±  1594.334  ops/s
// [info] LogWriteBench.asyncDurableStateLog  thrpt   25  103942.282 ±  1322.123  ops/s
// [info] LogWriteBench.asyncSmartTrieLog     thrpt   25  889826.683 ± 19652.398  ops/s
// [info] LogWriteBench.syncDiskStateLog      thrpt   25   19104.654 ±   982.393  ops/s
// [info] LogWriteBench.syncDurableStateLog   thrpt   25   18948.282 ±  2076.059  ops/s
// [info] LogWriteBench.syncSmartTrieLog      thrpt   25   24851.356 ±   218.865  ops/s
class LogWriteBench extends {
  import LogWriteBench._

  @Benchmark
  @Threads(1)
  def syncDiskStateLog(state: SyncDiskStateLog): Unit = {
    import state._
    log.addMessageBatch(newBatch(), new Array(0), nextID)
  }

  @Benchmark
  @Threads(1)
  def asyncDiskStateLog(state: AsyncDiskStateLog): Unit = {
    import state._
    log.addMessageBatch(newBatch(), new Array(0), nextID)
  }

  @Benchmark
  @Threads(1)
  def syncDurableStateLog(state: SyncDurableStateLog): Unit = {
    import state._
    log.addMessageBatch(newBatch(), new Array(0), nextID)
  }

  @Benchmark
  @Threads(1)
  def asyncDurableStateLog(state: AsyncDurableStateLog): Unit = {
    import state._
    log.addMessageBatch(newBatch(), new Array(0), nextID)
  }

  @Benchmark
  @Threads(1)
  def syncSmartTrieLog(state: SyncSmartTrieLog): Unit = {
    import state._
    log.append(LogEntry(nextID, newBatch()))
  }

  @Benchmark
  @Threads(1)
  def asyncSmartTrieLog(state: AsyncSmartTrieLog): Unit = {
    import state._
    log.append(LogEntry(nextID, newBatch()))
  }
}
