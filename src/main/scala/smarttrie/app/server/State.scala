package smarttrie.app.server

import java.util.concurrent.ConcurrentHashMap
import java.util.{TreeMap => JTreeMap}
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._
import smarttrie.atoms._

abstract class State(val allowConcurrentSnapshot: Boolean) {
  def get(key: Key): Option[Value]
  def remove(key: Key): Option[Value]
  def put(key: Key, value: Value): Option[Value]
  def foreach(f: (Key, Value) => Any): Unit
  def clear(): Unit
}

object State {

  def treeMap: State = new TreeMapState()
  def hashMap: State = new HashMapState()
  def trieMap: State = new TrieMapState()

  def apply(name: String): State =
    name match {
      case "tree-map" => treeMap
      case "hash-map" => hashMap
      case "trie-map" => trieMap
      case other =>
        throw new IllegalArgumentException(s"Invalid state name $other")
    }

  private final class TreeMapState extends State(allowConcurrentSnapshot = false) {

    private val state =
      new JTreeMap[Key, Value]()

    def get(key: Key): Option[Value] =
      Option(state.get(key))

    def remove(key: Key): Option[Value] =
      Option(state.remove(key))

    def put(key: Key, value: Value): Option[Value] =
      Option(state.put(key, value))

    def foreach(f: (Key, Value) => Any): Unit =
      state.forEach((k, v) => f(k, v))

    def clear(): Unit =
      state.clear()

    override def hashCode(): Int = 13 * state.hashCode()

    override def equals(obj: Any): Boolean =
      obj match {
        case other: TreeMapState => state == other.state
        case _                   => false
      }

    override def toString: String =
      state.entrySet().asScala.view.take(1_000).mkString("TreeMap<", ", ", ">")
  }

  private final class HashMapState extends State(allowConcurrentSnapshot = false) {

    private val state =
      new ConcurrentHashMap[Key, Value]()

    def get(key: Key): Option[Value] =
      Option(state.get(key))

    def remove(key: Key): Option[Value] =
      Option(state.remove(key))

    def put(key: Key, value: Value): Option[Value] =
      Option(state.put(key, value))

    def foreach(f: (Key, Value) => Any): Unit =
      state.forEach((key, value) => f(key, value))

    def clear(): Unit =
      state.clear()

    override def hashCode(): Int = 7 * state.hashCode()

    override def equals(obj: Any): Boolean =
      obj match {
        case other: HashMapState => state == other.state
        case _                   => false
      }

    override def toString: String =
      state.entrySet().asScala.view.take(1_000).mkString("HashMap<", ", ", ">")
  }

  private final class TrieMapState extends State(allowConcurrentSnapshot = true) {

    private val state =
      TrieMap.empty[Key, Value]

    def get(key: Key): Option[Value] =
      state.get(key)

    def remove(key: Key): Option[Value] =
      state.remove(key)

    def put(key: Key, value: Value): Option[Value] =
      state.put(key, value)

    def foreach(f: (Key, Value) => Any): Unit =
      state foreach { case (k, v) => f(k, v) }

    def clear(): Unit =
      state.clear()

    override def hashCode(): Int = 31 * state.hashCode()

    override def equals(obj: Any): Boolean =
      obj match {
        case other: TrieMapState => state == other.state
        case _                   => false
      }

    override def toString: String =
      state.view.take(1_000).mkString("TrieMap<", ", ", ">")
  }
}
