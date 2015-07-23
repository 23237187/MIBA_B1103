package zte.MBA.data.storage

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.json4s.JsonAST.JValue


private[MBA] case class PropTime(val d: JValue, val t: Long)
  extends Serializable

private[MBA] case class SetProp(
                               val fields: Map[String, PropTime],
                               val t: Long
                                 ) extends Serializable {
  def ++ (that: SetProp): SetProp = {
    val commonKeys = fields.keySet.intersect(that.fields.keySet)

    val common: Map[String, PropTime] = commonKeys.map { k =>
      val thisData = this.fields(k)
      val thatData = that.fields(k)

      val v = if (thisData.t > thatData.t) thisData else thatData
      (k, v)
    }.toMap

    val combinedFields = common ++
      (this.fields -- commonKeys) ++ (that.fields -- commonKeys)

    val combinedT = if (this.t > that.t) this.t else that.t

    SetProp(
      fields = combinedFields,
      t = combinedT
    )
  }
}

private[MBA] case class UnsetProp(fields: Map[String, Long])
  extends Serializable {
  def ++ (that: UnsetProp): UnsetProp = {
    val commonKeys = fields.keySet.intersect(that.fields.keySet)

    val common: Map[String, Long] = commonKeys.map { k =>
      val thisData = this.fields(k)
      val thatData = that.fields(k)

      val v = if (thisData > thatData) thisData else thatData
      (k, v)
    }.toMap

    val combinedFields = common ++
      (this.fields -- commonKeys) ++ (that.fields -- commonKeys)

    UnsetProp(
      fields = combinedFields
    )
  }
}

private[MBA] case class DeleteEntity (t: Long) extends Serializable {
  def ++ (that: DeleteEntity): DeleteEntity = {
    if (this.t > that.t) this else that
  }
}

private[MBA] case class EventOp (
                                val setProp: Option[SetProp] = None,
                                val unsetProp: Option[UnsetProp] = None,
                                val deleteEntity: Option[DeleteEntity] = None,
                                val firstUpdated: Option[DateTime] = None,
                                val lastUpdated: Option[DateTime] = None
                                ) extends Serializable {
  def ++ (that: EventOp): EventOp = {
    val firstUp = (this.firstUpdated ++ that.firstUpdated).reduceOption {
      (a, b) => if (b.getMillis < a.getMillis) b else a
    }
    val lastUp = (this.lastUpdated ++ that.lastUpdated).reduceOption {
      (a, b) => if (b.getMillis > a.getMillis) b else a
    }

    EventOp (
      setProp = (setProp ++ that.setProp).reduceOption(_++_),
      unsetProp = (unsetProp ++ that.unsetProp).reduceOption(_++_),
      deleteEntity = (deleteEntity ++ that.deleteEntity).reduceOption(_++_),
      firstUpdated = firstUp,
      lastUpdated = lastUp
    )
  }

  def toPropertyMap(): Option[PropertyMap] = {
    setProp.flatMap { set =>
      val unsetKeys: Set[String] = unsetProp.map( unset =>
        unset.fields.filter { case (k, v) => (v >= set.fields(k).t) }.keySet
      ).getOrElse(Set())

      val combinedFields = deleteEntity.map { delete =>
        if (delete.t >= set.t) {
          None
        } else {
          val deleteKeys: Set[String] = set.fields
            .filter { case (k, PropTime(kv, t)) =>
            (delete.t >= t)
          }.keySet
          Some(set.fields -- unsetKeys -- deleteKeys)
        }
      }.getOrElse{
        Some(set.fields -- unsetKeys)
      }

      combinedFields.map{ f =>
        require(firstUpdated.isDefined,
          "Unexpected Error: firstUpdated cannot be None.")
        require(lastUpdated.isDefined,
          "Unexpected Error: lastUpdated cannot be None.")
        PropertyMap(
          fields = f.mapValues(_.d).map(identity),
          firstUpdated = firstUpdated.get,
          lastUpdated = lastUpdated.get
        )
      }
    }
  }


}

private[MBA] object EventOp {
  def apply(e: Event): EventOp = {
    val t = e.eventTime.getMillis
    e.event match {
      case "$set" => {
        val fields = e.properties.fields.mapValues(jv =>
          PropTime(jv, t)
        ).map(identity)

        EventOp(
          setProp = Some(SetProp(fields = fields, t = t)),
          firstUpdated = Some(e.eventTime),
          lastUpdated = Some(e.eventTime)
        )
      }
      case "$unset" => {
        val fields = e.properties.fields.mapValues(jv => t).map(identity)
        EventOp(
          unsetProp = Some(UnsetProp(fields = fields)),
          firstUpdated = Some(e.eventTime),
          lastUpdated = Some(e.eventTime)
        )
      }
      case "$delete" => {
        EventOp(
          deleteEntity = Some(DeleteEntity(t)),
          firstUpdated = Some(e.eventTime),
          lastUpdated = Some(e.eventTime)
        )
      }
      case _ => {
        EventOp()
      }
    }
  }
}

private[MBA] object PEventAggregator {
  val eventNames = List("$set", "$unset", "$delete")

  def aggregateProperties(eventsRDD: RDD[Event]): RDD[(String, PropertyMap)] = {
    eventsRDD
      .map( e => (e.entityId, EventOp(e)) )
      .aggregateByKey[EventOp](EventOp())(
        seqOp = { case (u, v) => u ++ v},
        combOp = { case (accu, u) => accu ++ u}
      )
      .mapValues(_.toPropertyMap)
      .filter { case (k, v) => v.isDefined }
      .map { case (k, v) => (k, v.get)}
  }
}
