package benchmarks

import org.openjdk.jmh.annotations._
import smarttrie.app.server.{State => AppState}
import smarttrie.atoms._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object StateBench {

  val RecordSize = 4 * 1024 // 4kb

  @State(Scope.Benchmark)
  class BenchState {

    @Param(Array("tree-map", "trie-map"))
    var state: String = _

    @Param(Array("25600", "262144", "1310720"))
    var size: Int = _

    var keys: ArrayBuffer[Key] = _
    var values: ArrayBuffer[Value] = _
    var appState: AppState = _

    @Setup
    def setup(): Unit = {
      appState = AppState(state)
      keys = new ArrayBuffer(size)
      values = new ArrayBuffer(size)

      for (i <- 0 until size) {
        val key = Key(i.toString.getBytes())
        val value = Value(Random.nextBytes(RecordSize))
        appState.put(key, value)
        values += value
        keys += key
      }

      keys = Random.shuffle(keys)
      values = Random.shuffle(values)
    }
  }
}

// [info] Benchmark               (size)   (state)   Mode  Cnt        Score         Error  Units
// [info] StateBench.throughput    25600  tree-map  thrpt    9  1839965.612 ± 1189038.587  ops/s
// [info] StateBench.throughput    25600  trie-map  thrpt    9  2760493.478 ±   64591.277  ops/s
// [info] StateBench.throughput   262144  tree-map  thrpt    9   484690.679 ±   10343.074  ops/s
// [info] StateBench.throughput   262144  trie-map  thrpt    9  1081895.548 ±  189892.001  ops/s
// [info] StateBench.throughput  1310720  tree-map  thrpt    9   352065.384 ±  109470.738  ops/s
// [info] StateBench.throughput  1310720  trie-map  thrpt    9   683722.914 ±   14456.405  ops/s
class StateBench {
  import StateBench._

  @Benchmark
  @Fork(3)
  @Threads(1)
  @Warmup(iterations = 1)
  @Measurement(iterations = 3)
  def throughput(state: BenchState) = {
    import state._
    val keyIdx = Random.nextInt(size)
    val valueIdx = Random.nextInt(size)
    appState.put(keys(keyIdx), values(valueIdx))
  }
}
