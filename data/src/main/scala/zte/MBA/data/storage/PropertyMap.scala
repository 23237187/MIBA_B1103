package zte.MBA.data.storage

import org.joda.time.DateTime
import org.json4s.JValue
import org.json4s.JsonAST.JObject
import org.json4s.native.JsonMethods.parse


class PropertyMap(
  fields: Map[String, JValue],
  val firstUpdated: DateTime,
  val lastUpdated: DateTime
) extends DataMap(fields) {

  override
  def toString: String = s"PropertyMap(${fields}, ${firstUpdated}, ${lastUpdated})"

  override
  def hashCode: Int =
    41 * (
      41 * (
        41 + fields.hashCode
      ) + firstUpdated.hashCode
    ) + lastUpdated.hashCode

  override
  def equals(other: Any): Boolean = other match {
    case that: PropertyMap => {
      (that.canEqual(this)) &&
      (super.equals(that)) &&
      (this.firstUpdated.equals(that.firstUpdated)) &&
      (this.lastUpdated.equals(this.lastUpdated))
    }
    case that: DataMap => {
      super.equals(that)
    }
    case _ => false
  }

  override
  def canEqual(other: Any): Boolean = other.isInstanceOf[PropertyMap]

}

object PropertyMap {

  def apply(fields: Map[String, JValue],
    firstUpdated: DateTime,
    lastUpdated: DateTime): PropertyMap = {
    new PropertyMap(fields, firstUpdated, lastUpdated)
  }

  def apply(js: String, firstUpdated: DateTime, lastUpdated: DateTime): PropertyMap = {
    apply(
      fields = parse(js).asInstanceOf[JObject].obj.toMap,
      firstUpdated = firstUpdated,
      lastUpdated = lastUpdated
    )
  }
}
