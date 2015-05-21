package zte.MBA.data.storage

import org.joda.time.{DateTimeZone, DateTime}

/**
 * event存储中的每一个event都可以表示为这个case class中的一个字段
 * @param eventId event id
 * @param event event名
 * @param entityType event相关的实体类型
 * @param entityId event相关的实体Id
 * @param targetEntityType event相关的目标实体类型
 * @param targetEntityId event相关的目标实体Id
 * @param properties event属性
 * @param eventTime event发生的时间
 * @param tags tags
 * @param prId event的预测结果Id
 * @param creationTime 系统中event创建的时间
 * @group Event Data
 */
case class Event(
  val eventId: Option[String] = None,
  val event: String,
  val entityType: String,
  val entityId: String,
  val targetEntityType: Option[String] = None,
  val targetEntityId: Option[String] = None,
  val properties: DataMap = DataMap(),
  val eventTime: DateTime = DateTime.now,
  val tags: Seq[String] = Nil,
  val prId: Option[String] = None,
  val creationTime: DateTime = DateTime.now
) {
  override def toString(): String = {
    s"Event(id=$eventId,event=$event,eType=$entityType,eId=$entityId," +
      s"tType=$targetEntityType,tId=$targetEntityId,p=$properties,t=$eventTime," +
      s"tags=$tags,pKey=$prId,ct=$creationTime)"
  }
}

/**
 * [[Event]]验证  Utilities
 */
object EventValidation {
  val defaultTimeZone = DateTimeZone.UTC

  /**
   * 检查event名是否包含保留前缀
   * @param name Event名
   * @return 包含保留字，返回true
   */
  def isReservedPrefix(name: String): Boolean = name.startsWith("$") ||
    name.startsWith("zma_")

  /**
   * 三个内置的特殊event
   */
  val specialEvents = Set("$set", "$unset", "delete")

  /**
   * 检查Event是否是特殊Event
   * @param name Event名
   * @return 是特殊， 返回true
   */
  def isSpecialEvents(name: String): Boolean = specialEvents.contains(name)

  /**
   * 验证event
   * @param e 待验证Event
   */
  def validate(e: Event): Unit = {

    require(!e.event.isEmpty, "event must not be empty.")
    require(!e.entityType.isEmpty, "entityType must not be empty string.")
    require(!e.entityId.isEmpty, "entityId must not be empty string.")
    require(e.targetEntityType.map(!_.isEmpty).getOrElse(true),
      "targetEntityType must not be empty string")
    require(e.targetEntityId.map(!_.isEmpty).getOrElse(true),
      "targetEntityId must not be empty string.")
    require(!((e.targetEntityType != None) && (e.targetEntityId == None)),
      "targetEntityType and targetEntityId must be specified together.")
    require(!((e.targetEntityType == None) && (e.targetEntityId != None)),
      "targetEntityType and targetEntityId must be specified together.")
    require(!((e.event == "$unset") && e.properties.isEmpty),
      "properties cannot be empty for $unset event")
    require(!isReservedPrefix(e.event) || isSpecialEvents(e.event),
      s"${e.event} is not a supported reserved event name.")
    require(!isSpecialEvents(e.event) ||
      ((e.targetEntityType == None) && (e.targetEntityId == None)),
      s"Reserved event ${e.event} cannot have targetEntity")
    require(!isReservedPrefix(e.entityType) ||
      isBuildinEntityTypes(e.entityType),
      s"The entityType ${e.entityType} is not allowed. " +
        s"'pio_' is a reserved name prefix.")
    require(e.targetEntityType.map{ t =>
      (!isReservedPrefix(t) || isBuildinEntityTypes(t))}.getOrElse(true),
      s"The targetEntityType ${e.targetEntityType.get} is not allowed. " +
        s"'pio_' is a reserved name prefix.")
    validateProperties(e)
  }

  val buildinEntityTypes: Set[String] = Set("zma_pr")
  val buildinProperties: Set[String] = Set()

  def isBuildinEntityTypes(name: String): Boolean = buildinEntityTypes.contains(name)

  def validateProperties(e: Event): Unit = {
    e.properties.keySet.foreach {
      k =>
        require(!isReservedPrefix(k) || buildinProperties.contains(k),
          s"The property ${k} is not allowed. " +
            s"'pio_' is a reserved name prefix.")
    }
  }

}
