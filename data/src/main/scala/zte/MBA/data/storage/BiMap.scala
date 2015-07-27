package zte.MBA.data.storage

import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap


class BiMap[K, V] private[MBA] (
  private val m: Map[K, V],
  private val i: Option[BiMap[V, K]] = None
  ) extends Serializable {

  val inverse: BiMap[V, K] = i.getOrElse {
    val rev = m.map(_.swap)
    require((rev.size == m.size),
      s"Failed to create reversed map. Cannot have duplicated values.")
    new BiMap(rev, Some(this))
  }

  def get(k: K): Option[V] = m.get(k)

  def getOrElse(k: K, default: => V): V = m.getOrElse(k, default)

  def contains(k: K): Boolean = m.contains(k)

  def apply(k: K): V = m.apply(k)

  def toMap: Map[K, V] = m

  def toSeq: Seq[(K, V)] = m.toSeq

  def size: Int = m.size

  def take(n: Int): BiMap[K, V] = BiMap(m.take(n))

  override def toString: String = m.toString
}

object BiMap {
  def apply[K, V](x: Map[K, V]): BiMap[K, V] = new BiMap(x)

  def stringLong(keys: Array[String]): BiMap[String, Long] = {
    val hm = HashMap(keys.zipWithIndex.map(t => (t._1, t._2.toLong)) : _*)
    new BiMap(hm)
  }

  def stringLong(keys: Set[String]): BiMap[String, Long] = {
    val hm = HashMap(keys.toSeq.zipWithIndex.map(t => (t._1, t._2.toLong)) : _*)
    new BiMap(hm)
  }

  def stringLong(keys: RDD[String]): BiMap[String, Long] = {
    stringLong(keys.distinct.collect)
  }

  def stringInt(keys: Set[String]): BiMap[String, Int] = {
    val hm = HashMap(keys.toSeq.zipWithIndex : _*)
    new BiMap(hm)
  }

  def stringInt(keys: Array[String]): BiMap[String, Int] = {
    val hm = HashMap(keys.zipWithIndex : _*)
    new BiMap(hm)
  }

  def stringInt(keys: RDD[String]): BiMap[String, Int] = {
    stringInt(keys.distinct.collect)
  }

  private[this] def stringDoubleImpl(keys: Seq[String]): BiMap[String, Double] = {
    val ki = keys.zipWithIndex.map(e => (e._1, e._2.toDouble))
    new BiMap(HashMap(ki: _*))
  }

  def stringDouble(keys: Set[String]): BiMap[String, Double] = {
    stringDoubleImpl(keys.toSeq)
  }

  def stringDouble(keys: Array[String]): BiMap[String, Double] = {
    stringDoubleImpl(keys.toSeq)
  }

  def stringDouble(keys: RDD[String]): BiMap[String, Double] = {
    stringDoubleImpl(keys.distinct.collect)
  }


}
