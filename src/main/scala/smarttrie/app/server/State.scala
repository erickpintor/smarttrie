package smarttrie.app.server

import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import smarttrie.atoms._

trait State {
  def concurrentSnapshot: Boolean
  def get(key: Key): Option[Value]
  def remove(key: Key): Option[Value]
  def put(key: Key, value: Value): Option[Value]
  def foreach(f: (Key, Value) => Any): Unit
  def clear(): Unit
}

object State {

  def hashMap: State = new HashMap()
  def trieMap: State = new TrieMap()

  private final class HashMap extends State {
    val concurrentSnapshot: Boolean = false
    private[this] val state = new ConcurrentHashMap[Key, Value]()
    def get(key: Key): Option[Value] = Option(state.get(key))
    def remove(key: Key): Option[Value] = Option(state.remove(key))
    def put(key: Key, value: Value): Option[Value] = Option(state.put(key, value))
    def foreach(f: (Key, Value) => Any): Unit = state.forEach((k, v) => f(k, v))
    def clear(): Unit = state.clear()
  }

  private final class TrieMap extends State {
    val concurrentSnapshot: Boolean = true
    private[this] val state = new concurrent.TrieMap[Key, Value]()
    def get(key: Key): Option[Value] = state.get(key)
    def remove(key: Key): Option[Value] = state.remove(key)
    def put(key: Key, value: Value): Option[Value] = state.put(key, value)
    def foreach(f: (Key, Value) => Any): Unit =
      state foreach { case (k, v) => f(k, v) }
    def clear(): Unit = state.clear()
  }
}
