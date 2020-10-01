package smarttrie.app.server

import java.util.concurrent.ConcurrentHashMap
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

  def hashMap: State = new HashMapState()
  def trieMap: State = new TrieMapState()

  private final class HashMapState extends State(allowConcurrentSnapshot = false) {

    private[this] val state =
      new ConcurrentHashMap[Key, Value]()

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
  }

  private final class TrieMapState extends State(allowConcurrentSnapshot = true) {

    private[this] val state =
      new TrieMap[Key, Value]()

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
  }
}
