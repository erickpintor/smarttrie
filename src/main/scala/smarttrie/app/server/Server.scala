package smarttrie.app.server

import bftsmart.statemanagement.strategy.StandardStateManager
import bftsmart.statemanagement.{ApplicationState, StateManager}
import bftsmart.tom.server.{BatchExecutable, Recoverable}
import bftsmart.tom.{MessageContext, ReplicaContext, ServiceReplica}
import java.nio.file.{Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import org.slf4j.LoggerFactory
import scala.util.control.NonFatal
import smarttrie.atoms._
import smarttrie.io._

object Server {
  val CheckpointPeriod = 100_000
  val DataPath = Paths.get("files")

  def main(args: Array[String]): Unit = {
    val server = new Server(State(args(1)), DataPath, CheckpointPeriod)
    val replica = new ServiceReplica(args(0).toInt, server, server)
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      replica.kill()
      server.shutdown()
    }))
  }
}

final class Server(
    private[this] val state: State,
    private[this] val dataPath: Path,
    private[this] val checkpointPeriod: Int
) extends BatchExecutable
    with Recoverable {
  import Command._
  import Reply._

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] var checkpointThread: ExecutorService = _
  private[this] var isCheckpointing: AtomicBoolean = _
  private[this] var firstCID = CID.Null
  private[this] val log = Log(dataPath)

  if (state.allowConcurrentSnapshot) {
    checkpointThread = Executors.newSingleThreadExecutor()
    isCheckpointing = new AtomicBoolean(false)
  }
  loadReplicaState()

  def executeUnordered(request: Request, ctx: MessageContext): Array[Byte] =
    execute(request)

  def executeBatch(batch: Batch, ctxs: Array[MessageContext]): Array[Array[Byte]] = {
    var lastBatchCID = CID(ctxs(0).getConsensusId)
    val pending = Array.newBuilder[Request]
    val replies = Array.newBuilder[Array[Byte]]
    replies.sizeHint(batch.length)

    def executePending(): Unit = {
      val entries = pending.result()
      if (entries.nonEmpty) {
        log.append(LogEntry(lastBatchCID, entries))
        entries foreach { replies += execute(_) }
        maybeCheckpoint(lastBatchCID)
        pending.clear()
      }
    }

    for (i <- batch.indices) {
      val ctx = ctxs(i)
      val request = batch(i)
      val currentCID = CID(ctx.getConsensusId)
      if (currentCID > lastBatchCID) {
        executePending()
        lastBatchCID = currentCID
      }
      pending += request
    }

    executePending()
    replies.result()
  }

  private def execute(request: Request): Array[Byte] = {
    def execute0(): Array[Byte] = {
      val cmd = Codec.decode(request).as[Command]
      val res = runCommand(cmd)
      Codec.encode(res)
    }

    def runCommand(cmd: Command): Reply = {
      val res = cmd match {
        case Get(k)    => state.get(k)
        case Set(k, v) => state.put(k, v)
        case Remove(k) => state.remove(k)
      }
      res.map(Data).getOrElse(Null)
    }

    try execute0()
    catch {
      case NonFatal(err) =>
        logger.error("Error while executing command", err)
        Codec.encode(Error)
    }
  }

  private def loadReplicaState(): Unit = {
    firstCID = Checkpoint.read(dataPath) match {
      case Some(ckp) =>
        logger.info("Recovering replica checkpoint until {}", ckp.lastCID)
        state.clear()
        ckp.entries() foreach {
          case (key, value) =>
            state.put(key, value)
        }
        ckp.lastCID

      case None =>
        logger.info("No previews checkpoints to restore")
        CID.Null
    }

    logger.info("Re-executing remaining commands from log")
    log.entries(from = firstCID) foreach { entry =>
      entry.batch.foreach(execute)
      firstCID = entry.cid
    }

    logger.info("Replica state has been restored with last id {}", firstCID)
  }

  private def maybeCheckpoint(lastCID: CID): Unit = {
    def checkpoint0(): Unit = {
      val start = System.currentTimeMillis()
      logger.info("Starting replica checkpoint at {}", lastCID)
      Checkpoint.write(dataPath, state, lastCID)

      val delta = System.currentTimeMillis() - start
      logger.info("Checkpoint created after {}s", delta / 1000)

      log.truncate(lastCID)
      if (state.allowConcurrentSnapshot) {
        isCheckpointing.set(false)
      }
    }

    if (lastCID.toInt > 0 && lastCID.toInt % checkpointPeriod == 0) {
      if (state.allowConcurrentSnapshot) {
        if (isCheckpointing.compareAndSet(false, true)) {
          checkpointThread.execute(() => checkpoint0())
        } else {
          logger.warn("Skipping a checkpoint at {}", lastCID)
        }
      } else {
        checkpoint0()
      }
    }
  }

  def shutdown(): Unit = {
    log.close()
    if (state.allowConcurrentSnapshot) {
      checkpointThread.shutdown()
      checkpointThread.awaitTermination(5, TimeUnit.MINUTES)
      checkpointThread.shutdownNow()
    }
  }

  // Satisfy recoverable interface ------------------------------------------------

  private final class InternalStateManager extends StandardStateManager {
    def tom = tomLayer // expose tom layer so we can control its last seen CID
  }

  private[this] lazy val internalStateManager =
    new InternalStateManager()

  def getStateManager: StateManager =
    internalStateManager

  def setReplicaContext(replicaContext: ReplicaContext): Unit = {
    // Trick BFT into thinking this replica is up to date and can start processing
    // requests starting at this CID. Only works on single replica environments.
    internalStateManager.tom.setLastExec(firstCID.toInt)
    internalStateManager.setLastCID(firstCID.toInt)
    getStateManager.askCurrentConsensusId()
  }

  def getState(cid: Int, sendState: Boolean): ApplicationState =
    throw new UnsupportedOperationException()

  def setState(appState: ApplicationState): Int =
    throw new UnsupportedOperationException()

  def Op(cid: Int, request: Request, ctx: MessageContext): Unit = ()
  def noOp(cid: Int, batch: Batch, ctxs: Array[MessageContext]): Unit = ()
}
