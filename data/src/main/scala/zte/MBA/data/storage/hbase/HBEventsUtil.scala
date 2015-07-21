package zte.MBA.data.storage.hbase

import java.security.MessageDigest
import java.util.UUID
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.{read, write }

import zte.MBA.data.storage.{EventValidation, Event}


object HBEventsUtil {

  implicit val formats = DefaultFormats

  def tableName(namespace: String, appId: Int, channelId: Option[Int] = None): String = {
    channelId.map { ch =>
      s"${namespace}:events_${appId}_${ch}"
    }.getOrElse {
      s"${namespace}:events_${appId}"
    }
  }

  val colNames: Map[String, Array[Byte]] = Map(
    "event" -> "e",
    "entityType" -> "ety",
    "entityId" -> "eid",
    "targetEntityType" -> "tety",
    "targetEntityId" -> "teid",
    "properties" -> "p",
    "prId" -> "prid",
    "eventTime" -> "et",
    "eventTimeZone" -> "etz",
    "creationTime" -> "ct",
    "creationTimeZone" -> "ctz"
  ).mapValues(Bytes.toBytes(_))

  def hash(entityType: String, entityId: String): Array[Byte] = {
    val s = entityType + "-" + entityId
    val md5 = MessageDigest.getInstance("MD5")
    md5.digest(Bytes.toBytes(s))
  }

  class RowKey(
              val b: Array[Byte]
                ) {
    require((b.size == 32), s"Incorrect b size: ${b.size}")
    lazy val entityHash: Array[Byte] = b.slice(0, 16)
    lazy val millis: Long = Bytes.toLong(b.slice(16, 24))
    lazy val uuidLow: Long = Bytes.toLong(b.slice(24, 32))

    lazy val toBytes: Array[Byte] = b

    override def toString: String = {
      Base64.encodeBase64URLSafeString(toBytes)
    }
  }

  object RowKey {
    def apply(
             entityType: String,
             entityId: String,
             millis: Long,
             uuidLow: Long
               ): RowKey = {
      val b = hash(entityType, entityId) ++
        Bytes.toBytes(millis) ++ Bytes.toBytes(uuidLow)
      new RowKey(b)
    }

    def apply(s: String): RowKey = {
      try {
        apply(Base64.decodeBase64(s))
      } catch {
        case e: Exception => throw new RowKeyException(
          s"Failed to convert String ${s} to RowKey because ${e}", e)
      }
    }

    def apply(b: Array[Byte]): RowKey = {
      if (b.size != 32) {
        val bString = b.mkString(",")
        throw new RowKeyException(
          s"Incorrect byte array size. Bytes: ${bString}.")
      }
      new RowKey(b)
    }

  }

  class RowKeyException(val msg: String, val cause: Exception)
    extends Exception(msg, cause) {
      def this(msg: String) = this(msg, null)
  }

  case class PartialRowKey(entityType: String, entityId: String,
                            millis: Option[Long] = None) {
    val toBytes: Array[Byte] = {
      hash(entityType, entityId) ++
        (millis.map(Bytes.toBytes(_)).getOrElse(Array[Byte]()))
    }
  }

  def eventToPut(event: Event, appId: Int): (Put, RowKey) = {
    val rowKey = event.eventId.map { id =>
      RowKey(id)
    }.getOrElse {
      val uuidLow: Long = UUID.randomUUID().getLeastSignificantBits
      RowKey(
        entityType = event.entityType,
        entityId = event.entityId,
        millis = event.eventTime.getMillis,
        uuidLow = uuidLow
      )
    }

    val eBytes = Bytes.toBytes("e")

    val put = new Put(rowKey.toBytes, event.eventTime.getMillis)

    def addStringToE(col: Array[Byte], v: String): Put = {
      put.add(eBytes, col, Bytes.toBytes(v))
    }

    def addLongToE(col: Array[Byte], v: Long): Put = {
      put.add(eBytes, col, Bytes.toBytes(v))
    }

    addStringToE(colNames("event"), event.event)
    addStringToE(colNames("entityType"), event.entityType)
    addStringToE(colNames("entityId"), event.entityId)

    event.targetEntityType.foreach { targetEntityType =>
      addStringToE(colNames("targetEntityType"), targetEntityType)
    }

    event.targetEntityId.foreach { targetEntityId =>
      addStringToE(colNames("targetEntityId"), targetEntityId)
    }

    if (!event.properties.isEmpty) {
      addStringToE(colNames("properties"), write(event.properties.toJObject()))
    }

    event.prId.foreach { prId =>
      addStringToE(colNames("prId"), prId)
    }

    addLongToE(colNames("eventTime"), event.eventTime.getMillis)
    val eventTimeZone = event.eventTime.getZone
    if (!eventTimeZone.equals(EventValidation.defaultTimeZone)) {
      addStringToE(colNames("eventTimeZone"), eventTimeZone.getID)
    }

  }


}
