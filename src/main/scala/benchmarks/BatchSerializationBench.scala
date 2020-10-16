package benchmarks

import bftsmart.tom.server.defaultservices.CommandsInfo
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}
import smarttrie.app.server._
import smarttrie.atoms._
import smarttrie.io._

object BatchSerializationBench {
  @State(Scope.Thread)
  class DirectMemoryState {
    val buffer = ByteBuffer.allocateDirect(4 * 1024) // 4KB
  }

  @State(Scope.Thread)
  class HeapMemoryState {
    val buffer = ByteBuffer.allocate(4 * 1024) // 4KB
  }
}
// [info] Benchmark                                                       Mode  Cnt        Score       Error  Units
// [info] BatchSerializationBench.bftBatchSerialization                  thrpt    5   296838.831 ±  9765.254  ops/s
// [info] BatchSerializationBench.codecDirectMemBasedBatchSerialization  thrpt    5  1742504.621 ± 26748.860  ops/s
// [info] BatchSerializationBench.codecHeapMemBasedBatchSerialization    thrpt    5  1697685.274 ± 29275.414  ops/s
class BatchSerializationBench {
  import BatchSerializationBench._

  @Benchmark
  def bftBatchSerialization(): Array[Byte] = {
    val commandsInfo = new CommandsInfo(newBatch(), new Array(0))
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(commandsInfo)
    oos.flush()

    val batchBytes = bos.toByteArray
    val bf = ByteBuffer.allocate(12 + batchBytes.length)
    bf.putInt(batchBytes.length)
    bf.put(batchBytes)
    bf.putInt(0)
    bf.putInt(0)
    bf.array()
  }

  @Benchmark
  def codecDirectMemBasedBatchSerialization(state: DirectMemoryState): ByteBuffer = {
    import state._
    buffer.clear()
    val entry = LogEntry(CID(0), newBatch())
    val result = Codec.encode(buffer, entry)
    result
  }

  @Benchmark
  def codecHeapMemBasedBatchSerialization(state: HeapMemoryState): ByteBuffer = {
    import state._
    buffer.clear()
    val entry = LogEntry(CID(0), newBatch())
    val result = Codec.encode(buffer, entry)
    result
  }

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
