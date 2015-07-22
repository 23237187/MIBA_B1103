package zte.MBA.data.storage.hbase

import java.security.MessageDigest
import java.util.UUID

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hbase.client.{Scan, Result, Put}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.{DateTimeZone, DateTime}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JObject
import org.json4s.native.Serialization.{read, write}
import zte.MBA.data.storage.{DataMap, Event, EventValidation}


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

    addLongToE(colNames("creationTime"), event.creationTime.getMillis)
    val creationTimeZone = event.creationTime.getZone
    if (!creationTimeZone.equals(EventValidation.defaultTimeZone)) {
      addStringToE(colNames("creationTimeZone"), creationTimeZone.getID)
    }

    (put, rowKey)

  }

  def resultToEvent(result: Result, appId: Int): Event = {
    val rowKey = RowKey(result.getRow())

    val eBytes = Bytes.toBytes("e")

    def getStringCol(col: String): String = {
      val r = result.getValue(eBytes, colNames(col))
      require(r != null,
        s"Failed to get value for column ${col}. " +
          s"RowKey: ${rowKey.toString} " +
          s"StringBinary: ${Bytes.toStringBinary(result.getRow())}.")

      Bytes.toString(r)
    }

    def getLongCol(col: String): Long = {
      val r = result.getValue(eBytes, colNames(col))
      require(r != null,
        s"Failed to get value for column ${col}. " +
          s"RowKey: ${rowKey.toString} " +
          s"StringBinary: ${Bytes.toStringBinary(result.getRow())}.")

      Bytes.toLong(r)
    }

    def getOptStringCol(col: String): Option[String] = {
      val r = result.getValue(eBytes, colNames(col))
      if (r == null) {
        None
      } else {
        Some(Bytes.toString(r))
      }
    }

    def getTimestamp(col: String): Long = {
      result.getColumnLatestCell(eBytes, colNames(col)).getTimestamp()
    }

    val event = getStringCol("event")
    val entityType = getStringCol("entityType")
    val entityId = getStringCol("entityId")
    val targetEntityType = getOptStringCol("targetEntityType")
    val targetEntityId = getOptStringCol("targetEntityId")
    val properties: DataMap = getOptStringCol("properties")
      .map(s => DataMap(read[JObject](s))).getOrElse(DataMap())
    val prId = getOptStringCol("prId")
    val eventTimeZone = getOptStringCol("eventTimeZone")
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)
    val eventTime = new DateTime(
      getLongCol("eventTime"), eventTimeZone
    )
    val creationTimeZone = getOptStringCol("creationTimeZone")
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)
    val creationTime: DateTime = new DateTime(
      getOptStringCol("creationTime"), creationTimeZone
    )

    Event(
      eventId = Some(RowKey(result.getRow()).toString),
      event = event,
      entityType = entityType,
      entityId = entityId,
      targetEntityType = targetEntityType,
      targetEntityId = targetEntityId,
      properties = properties,
      eventTime = eventTime,
      tags = Seq(),
      prId = prId,
      creationTime = creationTime
    )
  }

  def createScan(
                startTime: Option[DateTime] = None,
                untilTime: Option[DateTime] = None,
                entityType: Option[String] = None,
                entityId: Option[String] = None,
                eventNames: Option[Seq[String]] = None,
                targetEntityType: Option[Option[String]] = None,
                targetEntityId: Option[Option[String]] = None,
                reversed: Option[Boolean] = None
                  ): Scan = {

    val scan: Scan = new Scan()

    (entityType, entityId) match {
      case (Some(et), Some(eid)) => {
        val start = PartialRowKey(et, eid,
          startTime.map(_.getMillis)).toBytes

        val stop = PartialRowKey(et, eid,
          untilTime.map(_.getMillis)).toBytes

        if (reversed.getOrElse(false)) {
          scan.setStartRow(stop)
          scan.setStopRow(start)
          scan.setReversed(true)
        } else {
          scan.setStartRow(start)
          scan.setStopRow(stop)
        }
      }

      case (_, _) => {
        val minTime: Long = startTime.map(_.getMillis).getOrElse(0)
        val maxTime: Long = untilTime.map(_.getMillis).getOrElse(Long.MaxValue)
        scan.setTimeRange(minTime, maxTime)
        if (reversed.getOrElse(false)) {
          scan.setReversed(true)
        }
      }
    }

    val filters = new FilterList(FilterList.Operator.MUST_PASS_ALL)

    val eBytes = Bytes.toBytes("e")

    def createBinaryFilter(col: String, value: Array[Byte]): SingleColumnValueFilter = {
      val comp = new BinaryComparator(value)
      new SingleColumnValueFilter(
        eBytes, colNames(col), CompareOp.EQUAL, comp
      )
    }

    def createSkipRowIfColumnExistFilter(col: String): SkipFilter = {
      val comp = new BinaryComparator(colNames(col))
      val q = new QualifierFilter(CompareOp.NOT_EQUAL, comp)
      new SkipFilter(q)
      )
    }

    entityType.foreach { et =>
      val compType = new BinaryComparator(Bytes.toBytes(et))
      val filterType = new SingleColumnValueFilter(
        eBytes, colNames("EntityType"), CompareOp.EQUAL, compType
      )
      filters.addFilter(filterType)
    }

    entityId.foreach { eid =>
      val compType = new BinaryComparator(Bytes.toBytes(eid))
      val filterType = new SingleColumnValueFilter(
        eBytes, colNames("EntityId"), CompareOp.EQUAL, compType
      )
      filters.addFilter(filterType)
    }

    eventNames.foreach { eventsList =>
      val eventFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE)
      eventsList.foreach { e =>
        val compEvent = new BinaryComparator(Bytes.toBytes(e))
        val filterEvent = new SingleColumnValueFilter(
          eBytes, colNames("event"), CompareOp.EQUAL, compEvent
        )
        eventFilters.addFilter(filterEvent)
      }
      if (!eventFilters.getFilters().isEmpty) {
        filters.addFilter(eventFilters)
      }
    }

    targetEntityType.foreach { tetOpt =>
      if (tetOpt.isEmpty) {
        val filter = createSkipRowIfColumnExistFilter("targetEntityType")
        filters.addFilter(filter)
      } else {
        tetOpt.foreach { tet =>
          val filter = createBinaryFilter(
            "targetEntityType", Bytes.toBytes(tet)
          )
          filter.setFilterIfMissing(true)
          filters.addFilter(filter)
        }
      }
    }

    targetEntityId.foreach { teidOpt =>
      if (teidOpt.isEmpty) {
        val filter = createSkipRowIfColumnExistFilter("targetEntityId")
        filters.addFilter(filter)
      } else {
        teidOpt.foreach { teid =>
          val filter = createBinaryFilter(
            "targetEntityId", Bytes.toBytes(teid)
          )
          filter.setFilterIfMissing(true)
          filters.addFilter(filter)
        }
      }
    }

    if (!filters.getFilters().isEmpty) {
      scan.setFilter(filters)
    }

    scan
  }
}
