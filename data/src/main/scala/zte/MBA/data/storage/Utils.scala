package zte.MBA.data.storage

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

/**
 * Created by root on 15-7-27.
 */
class Utils {
  def addPrefixToAttributeKeys[T](
    attributes: Map[String, T],
    prefix: String = "ca_"): Map[String, T] = {
    attributes map { case (k, v) => (prefix + k, v) }
  }

  /** Remove prefix from custom attribute keys. */
  def removePrefixFromAttributeKeys[T](
    attributes: Map[String, T],
    prefix: String = "ca_"): Map[String, T] = {
    attributes map { case (k, v) => (k.stripPrefix(prefix), v) }
  }

  /**
   * Appends App ID to any ID.
   * Used for distinguishing different app's data within a single collection.
   */
  def idWithAppid(appid: Int, id: String): String = appid + "_" + id

  def stringToDateTime(dt: String): DateTime =
    ISODateTimeFormat.dateTimeParser.parseDateTime(dt)
}
