package zte.MBA.data.storage

import org.joda.time.{DateTimeZone, DateTime}

/**
 * event�洢�е�ÿһ��event�����Ա�ʾΪ���case class�е�һ���ֶ�
 * @param eventId event id
 * @param event event��
 * @param entityType event��ص�ʵ������
 * @param entityId event��ص�ʵ��Id
 * @param targetEntityType event��ص�Ŀ��ʵ������
 * @param targetEntityId event��ص�Ŀ��ʵ��Id
 * @param properties event����
 * @param eventTime event������ʱ��
 * @param tags tags
 * @param prId event��Ԥ����Id
 * @param creationTime ϵͳ��event������ʱ��
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
 * [[Event]]��֤  Utilities
 */
object EventValidation {
  val defaultTimeZone = DateTimeZone.UTC

  /**
   * ���event���Ƿ��������ǰ׺
   * @param name Event��
   * @return ���������֣�����true
   */
  def isReservedPrefix(name: String): Boolean = name.startsWith("$") ||
    name.startsWith("zma_")

  /**
   * �������õ�����event
   */
  val specialEvents = Set("$set", "$unset", "delete")

  /**
   * ���Event�Ƿ�������Event
   * @param name Event��
   * @return �����⣬ ����true
   */
  def isSpecialEvents(name: String): Boolean = specialEvents.contains(name)

  /**
   * ��֤event
   * @param e ����֤Event
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
