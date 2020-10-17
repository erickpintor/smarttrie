package smarttrie.lang

import io.netty.util.Recycler

object Pooled {

  def apply[A](fn: => A)(reset: A => Unit): Unit =
    new Pooled(fn, reset)
}

final class Pooled[A] private (newInstance: A, reset: A => Unit) {

  private final class Holder(handler: Recycler.Handle[Holder], val instance: A) {
    def recycle(): Unit = {
      reset(instance)
      handler.recycle(this)
    }
  }

  private[this] val pool = new Recycler[Holder]() {
    def newObject(handle: Recycler.Handle[Holder]): Holder =
      new Holder(handle, newInstance)
  }

  def apply[B](fn: A => B): B = {
    val holder = pool.get()
    val result = fn(holder.instance)
    holder.recycle()
    result
  }
}
