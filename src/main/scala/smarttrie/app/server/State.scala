package smarttrie.app.server

import java.util.{TreeMap => JTreeMap}
import scala.collection.concurrent.TrieMap
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
  def trieMap: State = new TrieMapState()

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
  }
}
