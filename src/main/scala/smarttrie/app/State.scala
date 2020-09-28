package smarttrie.app

import java.util
import scala.collection.concurrent
import smarttrie.atoms._

trait State {
  def clear(): Unit
  def get(key: Key): Option[Value]
  def remove(key: Key): Option[Value]
  def put(key: Key, value: Value): Option[Value]
  def foreach(f: (Key, Value) => Any): Unit
}

object State {

  def treeMap: State = new TreeMap()
  def trieMap: State = new TrieMap()

  private final class TreeMap extends State {
    private[this] val state = new util.TreeMap[Key, Value]()
    def clear(): Unit = state.clear()
    def get(key: Key): Option[Value] = Option(state.get(key))
    def remove(key: Key): Option[Value] = Option(state.remove(key))
    def put(key: Key, value: Value): Option[Value] = Option(state.put(key, value))
    def foreach(f: (Key, Value) => Any): Unit = state.forEach((k, v) => f(k, v))
  }

  private final class TrieMap extends State {
    private[this] val state = new concurrent.TrieMap[Key, Value]()
    def clear(): Unit = state.clear()
    def get(key: Key): Option[Value] = state.get(key)
    def remove(key: Key): Option[Value] = state.remove(key)
    def put(key: Key, value: Value): Option[Value] = state.put(key, value)
    def foreach(f: (Key, Value) => Any): Unit =
      state foreach { case (k, v) => f(k, v) }
  }
}
