package smarttrie.app.client

import bftsmart.tom.ServiceProxy
import java.util.concurrent.atomic.AtomicInteger
import java.util.{HashMap => JHashMap, Map => JMap, Set => JSet, Vector => JVector}
import org.slf4j.LoggerFactory
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import site.ycsb.{ByteArrayByteIterator, ByteIterator, DB, Status}
import smarttrie.atoms._
import smarttrie.io._
import smarttrie.lang._

object Client {

  private val logger =
    LoggerFactory.getLogger("client")

  private val index =
    new AtomicInteger(0)

  private lazy val proxies: Vector[ServiceProxy] = {
    val config = Source.fromFile("./config/hosts.config")
    val servers = Vector.newBuilder[ServiceProxy]

    config.getLines() foreach { line =>
      if (!line.startsWith("#")) {
        val parts = line.split(' ')
        val id = parts.head.trim
        servers += new ServiceProxy(id.toInt)
      }
    }

    servers.result()
  }
}

final class Client extends DB {
  import Client._
  import Command._
  import Reply._

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
    val k = theKey(key)
    val v = theValue(values)
    val res = execute(Set(k, v))
    toStatus(res)
  }

  def delete(table: String, key: String): Status = {
    val k = theKey(key)
    val res = execute(Remove(k))
    toStatus(res)
  }

  def read(
      table: String,
      key: String,
      fields: JSet[String],
      result: JMap[String, ByteIterator]
  ): Status = {
    val k = theKey(key)
    execute(Get(k)) match {
      case Null  => Status.OK
      case Error => Status.ERROR
      case Data(value) =>
        val bytes = new ByteArrayByteIterator(value.data)
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

  private def theKey(key: String): Key =
    Key(key.toUTF8Array)

  private def theValue(values: JMap[String, ByteIterator]): Value = {
    val (_, bytes) = values.asScala.head
    Value(bytes.toArray)
  }

  private def toStatus(reply: Reply): Status =
    reply match {
      case Null | Data(_) => Status.OK
      case Error          => Status.ERROR
    }

  private def execute(cmd: Command): Reply =
    try {
      val proxy = proxies(index.getAndIncrement() % proxies.size)
      val msg = Codec.encode(cmd)
      val res = proxy.invokeOrdered(msg)
      Codec.decode(res).as[Reply]
    } catch {
      case NonFatal(err) =>
        logger.error("Request failed", err)
        Error
    }
}
