package zte.MBA.data.storage

import org.joda.time.DateTime
import org.json4s.{CustomSerializer, MappingException, DefaultFormats}
import org.json4s.JsonAST._
import zte.MBA.data.{Utils => DataUtils}

object EventJson4sSupport {
  implicit val formats = DefaultFormats

  def readJson: PartialFunction[JValue, Event] = {
    case JObject(x) => {
      val fields = new DataMap(x.toMap)
      try {
        val event = fields.get[String]("event")
        val entityType = fields.get[String]("entityType")
        val entityId = fields.get[String]("entityId")
        val targetEntityType = fields.getOpt[String]("targetEntityType")
        val targetEntityId = fields.getOpt[String]("targetEntityId")
        val properties = fields.getOrElse[Map[String, JValue]](
          "properties", Map())
        lazy val currentTime = DateTime.now(EventValidation.defaultTimeZone)
        val eventTime = fields.getOpt[String]("eventTime")
          .map { s=>
            try {
              DataUtils.stringToDateTime(s)
            } catch {
              case _: Exception =>
                throw new MappingException(s"Fail to extract eventTime ${s}")
            }
        }.getOrElse(currentTime)

        val tag = List()

        val prId = fields.getOpt[String]("prId")

        val creationTime = currentTime

        val newEvent = Event(
          event = event,
          entityType = entityType,
          entityId = entityId,
          targetEntityType = targetEntityType,
          targetEntityId = targetEntityId,
          properties = DataMap(properties),
          eventTime = eventTime,
          prId = prId,
          creationTime = creationTime
        )
        EventValidation.validate(newEvent)
        newEvent
      } catch {
        case e: Exception => throw new MappingException(e.toString, e)
      }
    }
  }

  def writeJson: PartialFunction[Any, JValue] = {
    case d: Event => {
      JObject(
        JField("eventId",
          d.eventId.map( eid => JString(eid)).getOrElse(JNothing)) ::
          JField("event", JString(d.event)) ::
          JField("entityType", JString(d.entityType)) ::
          JField("entityId", JString(d.entityId)) ::
          JField("targetEntityType",
            d.targetEntityType.map(JString(_)).getOrElse(JNothing)) ::
          JField("targetEntityId",
            d.targetEntityId.map(JString(_)).getOrElse(JNothing)) ::
          JField("properties", d.properties.toJObject) ::
          JField("eventTime", JString(DataUtils.dateTimeToString(d.eventTime))) ::
          // disable tags from API for now
          // JField("tags", JArray(d.tags.toList.map(JString(_)))) ::
          // disable tags from API for now
          JField("prId",
            d.prId.map(JString(_)).getOrElse(JNothing)) ::
          // don't show creationTime for now
          JField("creationTime",
            JString(DataUtils.dateTimeToString(d.creationTime))) ::
          Nil)
    }
  }

  def deserializeFromJValue: PartialFunction[JValue, Event] = {
    case jv: JValue => {
      val event = (jv \ "event").extract[String]
      val entityType = (jv \ "entityType").extract[String]
      val entityId = (jv \ "entityId").extract[String]
      val targetEntityType = (jv \ "targetEntityType").extract[Option[String]]
      val targetEntityId = (jv \ "targetEntityId").extract[Option[String]]
      val properties = (jv \ "properties").extract[JObject]
      val eventTime = DataUtils.stringToDateTime(
        (jv \ "eventTime").extract[String])
      val tags = (jv \ "tags").extract[Seq[String]]
      val prId = (jv \ "prId").extract[Option[String]]
      val creationTime = DataUtils.stringToDateTime(
        (jv \ "creationTime").extract[String])
      Event(
        event = event,
        entityType = entityType,
        entityId = entityId,
        targetEntityType = targetEntityType,
        targetEntityId = targetEntityId,
        properties = DataMap(properties),
        eventTime = eventTime,
        tags = tags,
        prId = prId,
        creationTime = creationTime)
    }
  }

  def serializeToJValue: PartialFunction[Any, JValue] = {
    case d: Event => {
      JObject(
        JField("event", JString(d.event)) ::
          JField("entityType", JString(d.entityType)) ::
          JField("entityId", JString(d.entityId)) ::
          JField("targetEntityType",
            d.targetEntityType.map(JString(_)).getOrElse(JNothing)) ::
          JField("targetEntityId",
            d.targetEntityId.map(JString(_)).getOrElse(JNothing)) ::
          JField("properties", d.properties.toJObject) ::
          JField("eventTime", JString(DataUtils.dateTimeToString(d.eventTime))) ::
          JField("tags", JArray(d.tags.toList.map(JString(_)))) ::
          JField("prId",
            d.prId.map(JString(_)).getOrElse(JNothing)) ::
          JField("creationTime",
            JString(DataUtils.dateTimeToString(d.creationTime))) ::
          Nil)
    }
  }

  class DBSerializer extends CustomSerializer[Event](formats => (
    deserializeFromJValue, serializeToJValue))

  class APISerializer extends CustomSerializer[Event](formats => (
    readJson, writeJson))
}
