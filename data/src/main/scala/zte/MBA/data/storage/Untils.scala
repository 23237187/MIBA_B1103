package zte.MBA.data.storage

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat


private[MBA] object Utils {
  def addPrefixToAttributeKeys[T](
    attributes: Map[String, T],
    prefix: String = "ca_"): Map[String, T] = {
    attributes.map {
      case (k, v) => (prefix + k, v)
    }
  }

  def removePrefixFromAttributeKeys[T](
    attributes: Map[String, T],
    prefix: String = "ca_"): Map[String, T] = {
    attributes.map {
      case (k, v) => (k.stripPrefix(prefix), v)
    }
  }

  def idWithAppId(appId: Int, id: String): String = appId + "_" + id

  def stringToDateTime(dt: String): DateTime =
    ISODateTimeFormat.dateTimeParser.parseDateTime(dt)
}
