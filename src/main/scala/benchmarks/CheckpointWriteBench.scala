package benchmarks

import bftsmart.tom.server.defaultservices.DiskStateLog
import bftsmart.tom.server.defaultservices.durability.DurableStateLog
import java.nio.file.Paths
import org.openjdk.jmh.annotations.{State, _}
import scala.util.Random
import smarttrie.app.server.{State => AppState, _}
import smarttrie.atoms._
import smarttrie.io._

object CheckpointWriteBench {

  val ckpDir = Paths.get("files")

  @State(Scope.Benchmark)
  class FixedSizeState {
    val state = AppState.treeMap

    @Setup
    def setup(): Unit = {
      IO.cleanDirectory(ckpDir, removeDir = false)

      for (_ <- 0 until 1_000_000) {
        val key = Key(Random.nextBytes(32))
        val value = Value(Random.nextBytes(64))
        state.put(key, value)
      }
    }
  }
}

// [info] Benchmark                                           Mode  Cnt  Score   Error  Units
// [info] CheckpointWriteBench.bftDiskStateLogCheckpoint     thrpt   25  2.988 ± 0.092  ops/s
// [info] CheckpointWriteBench.bftDurableStateLogCheckpoint  thrpt   25  2.973 ± 0.044  ops/s
// [info] CheckpointWriteBench.smartTrieWriteCheckpoint      thrpt   25  3.231 ± 0.077  ops/s
class CheckpointWriteBench {
  import CheckpointWriteBench._

  @Benchmark
  @Threads(1)
  def bftDurableStateLogCheckpoint(state: FixedSizeState): Unit = {
    val log =
      new DurableStateLog(
        0,
        Array.emptyByteArray,
        Array.emptyByteArray,
        false,
        false,
        false
      )

    val data =
      Codec.writer { out =>
        state.state foreach {
          case (key, value) =>
            out
              .write(key)
              .write(value)
        }
      }

    log.newCheckpoint(data, Array.emptyByteArray, 0)
  }

  @Benchmark
  @Threads(1)
  def bftDiskStateLogCheckpoint(state: FixedSizeState): Unit = {
    val log =
      new DiskStateLog(
        0,
        Array.emptyByteArray,
        Array.emptyByteArray,
        false,
        false,
        false
      )

    val data =
      Codec.writer { out =>
        state.state foreach {
          case (key, value) =>
            out
              .write(key)
              .write(value)
        }
      }

    log.newCheckpoint(data, Array.emptyByteArray, 0)
  }

  @Benchmark
  @Threads(1)
  def smartTrieWriteCheckpoint(state: FixedSizeState): Unit = {
    Checkpoint.write(ckpDir, state.state, CID(999))
  }
}
