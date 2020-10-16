package smarttrie.app.server

import smarttrie.test._
import smarttrie.atoms._
import scala.util.Random
import java.nio.file.{Files, Path}
import smarttrie.io.IO

class CheckpointSpec extends Spec {

  var ckpDir: Path = _

  before {
    ckpDir = Files.createTempDirectory("smarttrie-ckp")
  }

  after {
    if (ckpDir ne null) {
      IO.cleanDirectory(ckpDir)
    }
  }

  "Checkpoint" should "write/read checkpoints" in {
    val oldState = State.trieMap
    for (_ <- 0 until 10_000) {
      val key = Key(Random.nextBytes(32))
      val value = Value(Random.nextBytes(128))
      oldState.put(key, value)
    }

    Checkpoint.write(ckpDir, oldState, CID(9_999), blockSizeMB = 1)

    val newState = State.trieMap
    val reader = Checkpoint.read(ckpDir).get
    reader.lastCID shouldBe CID(9_999)

    reader.entries() foreach {
      case (key, value) =>
        newState.put(key, value)
    }

    newState should equal(oldState)
  }
}
