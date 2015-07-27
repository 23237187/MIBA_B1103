package zte.MBA.data.storage

import org.apache.spark.rdd.RDD


class EntityIdIxMap(val idToIx: BiMap[String, Long]) extends Serializable {
  val ixToId: BiMap[Long, String] = idToIx.inverse

  def apply(id: String): Long = idToIx(id)

  def apply(ix: Long): String = ixToId(ix)

  def contains(id: String): Boolean = idToIx.contains(id)

  def contains(ix: Long): Boolean = ixToId.contains(ix)

  def get(id: String): Option[Long] = idToIx.get(id)

  def get(ix: Long): Option[String] = ixToId.get(ix)

  def getOrElse(id: String, default: => Long): Long =
    idToIx.getOrElse(id, default)

  def getOrElse(ix: Long, default: => String): String =
    ixToId.getOrElse(ix, default)

  def toMap: Map[String, Long] = idToIx.toMap

  def size: Long = idToIx.size

  def take(n: Int): EntityIdIxMap = new EntityIdIxMap(idToIx.take(n))

  override def toString: String = idToIx.toString
}

object EntityIdIxMap {
  def apply(keys: RDD[String]): EntityIdIxMap = {
    new EntityIdIxMap(BiMap.stringLong(keys))
  }
}

class EntityMap[A](val idToData: Map[String, A],
  override val idToIx: BiMap[String, Long]) extends EntityIdIxMap(idToIx) {

  def this(idToData: Map[String, A]) = this(
    idToData,
    BiMap.stringLong(idToData.keySet)
  )

  def data(id:String): A = idToData(id)

  def data(ix: Long): A = idToData(ixToId(ix))

  def getData(id: String): Option[A] = idToData.get(id)

  def getData(ix: Long): Option[A] = idToData.get(ixToId(ix))

  def getOrElseData(id: String, default: => A): A =
    getData(id).getOrElse(default)

  def getOrElseData(ix: Long, default: => A): A =
    getData(ix).getOrElse(default)

  override def take(n: Int): EntityMap[A] = {
    val newIdToIx = idToIx.take(n)
    new EntityMap[A](idToData.filterKeys(newIdToIx.contains(_)), newIdToIx)
  }

  override def toString: String = {
    s"idToData: ${idToData.toString} " + s"idToix: ${idToIx.toString}"
  }
}


