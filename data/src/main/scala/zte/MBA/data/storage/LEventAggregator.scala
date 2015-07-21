package zte.MBA.data.storage

import org.joda.time.DateTime

object LEventAggregator {

  def aggregateProperties(events: Iterator[Event]): Map[String, PropertyMap] = {
    events.toList
      .groupBy(_.entityId)
      .mapValues(_.sortBy(_.eventTime.getMillis)
        .foldLeft[Prop](Prop())(propAggregator))
      .filter{ case (k, v) => v.dm.isDefined }
      .mapValues{ v =>
        require(v.firstUpdated.isDefined,
          "Unexpected Error: firstUpdated cannot be None.")
        require(v.lastUpdated.isDefined,
          "Unexpected Error: lastUpdated cannot be None.")

        PropertyMap(
          fields = v.dm.get.fields,
          firstUpdated = v.firstUpdated.get,
          lastUpdated = v.lastUpdated.get
        )
      }
  }

  def aggregatePropertiesSingle(events: Iterator[Event])
  : Option[PropertyMap] = {
    val prop = events.toList
      .sortBy(_.eventTime.getMillis)
      .foldLeft[Prop](Prop())(propAggregator)

    prop.dm.map{ d =>
      require(prop.firstUpdated.isDefined,
        "Unexpected Error: firstUpdated cannot be None.")
      require(prop.lastUpdated.isDefined,
        "Unexpected Error: lastUpdated cannot be None.")

      PropertyMap(
        fields = d.fields,
        firstUpdated = prop.firstUpdated.get,
        lastUpdated = prop.lastUpdated.get
      )
    }
  }

  val eventNames = List("$set", "$unset", "$delete")

  private
  def dataMapAggregator: ((Option[DataMap], Event) => Option[DataMap]) = {
    (p, e) => {
      e.event match {
        case "$set" => {
          if (p == None) {
            Some(e.properties)
          } else {
            p.map(_ ++ e.properties)
          }
        }
        case "$unset" => {
          if (p == None) {
            None
          } else {
            p.map(_ -- e.properties.keySet)
          }
        }
        case "$delete" => None
        case _ => p
      }
    }
  }

  private
  def propAggregator: ((Prop, Event) => Prop) = {
    (p, e) => {
      e.event match {
        case "$set" | "$unset" | "delete" => {
          Prop(
            dm = dataMapAggregator(p.dm, e),
            firstUpdated = p.firstUpdated.map { t =>
              first(t, e.eventTime)
            }.orElse(Some(e.eventTime)),
            lastUpdated = p.lastUpdated.map { t =>
              last(t, e.eventTime)
            }.orElse(Some(e.eventTime))
          )
        }
        case _ => p
      }
    }
  }

  private
  def first(a: DateTime, b: DateTime): DateTime = if (b.isBefore(a)) b else a

  private
  def last(a: DateTime, b: DateTime): DateTime = if (b.isAfter(a)) b else a

  private case class Prop(
    dm: Option[DataMap] = None,
    firstUpdated: Option[DateTime] = None,
    lastUpdated: Option[DateTime] = None
    )


}
