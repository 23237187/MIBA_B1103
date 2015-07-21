package zte.MBA.data.storage

import org.joda.time.DateTime

/**
 * 提供将[[Event]]s 聚合为 [[LEvents]]的服务
 *将一组对特定对象的一系列属性操作市委一串events, 则将这些events的效果按时序逻辑叠加,得到的状态既是对象最终状态,称为聚合
 * events -> 状态
 */
object LEventAggregator {

  /**
   * 从一个events集合中,将每个独特的EntityId代表的实体进行聚合,确定实体的状态
   * @param events 待聚合事件集合
   * @return 一个Map key是EntityId, value是一个PropertyMap,代表该实体的状态
   */
  def aggregateProperties(events: Iterator[Event]): Map[String, PropertyMap] = {
    events.toList
      .groupBy(_.entityId)//按eventityID成组, key是entityID
      .mapValues(_.sortBy(_.eventTime.getMillis)//mapValues 只对 Value操作
        .foldLeft[Prop](Prop())(propAggregator))//计算每个entityID的状态,聚合.状态是一个Prop对象 Map[entityId, Prop]
      .filter{ case (k, v) => v.dm.isDefined }//去掉空的状态
      .mapValues{ v => //v是一个Prop对象
        require(v.firstUpdated.isDefined,
          "Unexpected Error: firstUpdated cannot be None.")
        require(v.lastUpdated.isDefined,
          "Unexpected Error: lastUpdated cannot be None.")

        PropertyMap(
          fields = v.dm.get.fields,
          firstUpdated = v.firstUpdated.get,
          lastUpdated = v.lastUpdated.get
        )//Prop -> PropertyMap
      }
  }

  /**
   * 针对单独entityId的聚合版本
   * @param events
   * @return
   */
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

  //能够实际对状态产生影响的事件类型
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
