package zte.MBA.data.storage

import org.joda.time.DateTime
import org.json4s.{CustomSerializer, DefaultFormats}
import org.json4s.JsonAST.{JString, JValue}

/**
 * Joda-TimeµÄJSON4S ÐòÁÐ»¯
 */
object DateTimeJson4sSupport {

  @transient lazy implicit val format = DefaultFormats

  def serializeToJValue: PartialFunction[Any, JValue] = {
    case d: DateTime => JString(DataUtils.dateTimeToString(d))
  }

  def deserializeFromJValue: PartialFunction[JValue, DateTime] = {
    case jv: JValue => DataUtils.stringToDataTime(jv.extract[String])
  }

  class Serializer extends CustomSerializer[DateTime](
    format => (deserializeFromJValue, serializeToJValue)
  )
}
