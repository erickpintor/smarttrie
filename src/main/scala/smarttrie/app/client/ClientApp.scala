package smarttrie.app.client

import bftsmart.tom.ServiceProxy
import io.netty.buffer.Unpooled
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.{Level, Logger}
import java.util.{HashMap => JHashMap, Map => JMap, Set => JSet, Vector => JVector}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import site.ycsb.{ByteArrayByteIterator, ByteIterator, DB, Status}
import smarttrie.atoms._
import smarttrie.io._
import smarttrie.lang._

final class ClientApp extends DB {
  import Command._
  import Reply._

  private[this] val logger = Logger.getLogger("client")
  private[this] val cache = new ConcurrentHashMap[String, Key]()
  private[this] var proxy: ServiceProxy = _

  override def init(): Unit = {
    val serverID = getProperties.getProperty("serverID")
    if (serverID eq null) {
      throw new IllegalStateException("Missing server id")
    }
    proxy = new ServiceProxy(serverID.toInt)
  }

  def insert(
      table: String,
      key: String,
      values: JMap[String, ByteIterator]
  ): Status = update(table, key, values)

  def update(
      table: String,
      key: String,
      values: JMap[String, ByteIterator]
  ): Status = {
    val k = cachedKey(key)
    val v = singleValue(values)
    val res = execute(Set(k, v))
    toStatus(res)
  }

  def delete(table: String, key: String): Status = {
    val k = cachedKey(key)
    val res = execute(Remove(k))
    toStatus(res)
  }

  def read(
      table: String,
      key: String,
      fields: JSet[String],
      result: JMap[String, ByteIterator]
  ): Status = {
    val k = cachedKey(key)
    execute(Get(k)) match {
      case Null  => Status.OK
      case Error => Status.ERROR
      case Data(value) =>
        val bytes = new ByteArrayByteIterator(value.buf.toByteArray)
        result.put("field0", bytes)
        Status.OK
    }
  }

  def scan(
      table: String,
      startKey: String,
      recordCount: Int,
      fields: JSet[String],
      results: JVector[JHashMap[String, ByteIterator]]
  ): Status =
    throw new UnsupportedOperationException

  private def cachedKey(key: String): Key =
    cache.computeIfAbsent(key, (str: String) => Key(str.toBuf))

  private def singleValue(values: JMap[String, ByteIterator]): Value = {
    val (_, bytes) = values.asScala.head
    Value(Unpooled.wrappedBuffer(bytes.toArray))
  }

  private def toStatus(reply: Reply): Status =
    reply match {
      case Null | Data(_) => Status.OK
      case Error          => Status.ERROR
    }

  private def execute(cmd: Command): Reply =
    try {
      val msg = Codec.encode(cmd)
      val res = proxy.invokeUnordered(msg.toByteArray)
      Codec.decode(res).as[Reply]
    } catch {
      case NonFatal(err) =>
        logger.log(Level.SEVERE, "Request failed", err)
        Error
    }

}
