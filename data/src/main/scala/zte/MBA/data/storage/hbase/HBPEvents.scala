package zte.MBA.data.storage.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, MBAHBaseUtil, TableInputFormat}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import zte.MBA.data.storage.{Event, PEvents, StorageClientConfig}


class HBPEvents(client: HBClient, config: StorageClientConfig, namespace: String) extends PEvents {

  def checkTableExists(appId: Int, channelId: Option[Int]): Unit = {
    if (!client.admin.tableExists(HBEventsUtil.tableName(namespace, appId, channelId))) {
      if (channelId.nonEmpty) {
        logger.error(s"The appId $appId with channelId $channelId does not exist." +
          s" Please use valid appId and channelId.")
        throw new Exception(s"HBase table not found for appId $appId" +
          s" with channelId $channelId.")
      } else {
        logger.error(s"The appId $appId does not exist. Please use valid appId.")
        throw new Exception(s"HBase table not found for appId $appId.")
      }
    }
  }

  override
  def find(
          appId: Int,
          channelId: Option[Int] = None,
          startTime: Option[DateTime] = None,
          untilTime: Option[DateTime] = None,
          entityType: Option[String] = None,
          entityId: Option[String] = None,
          eventNames: Option[Seq[String]] = None,
          targetEntityType: Option[Option[String]] = None,
          targetEntityId: Option[Option[String]] = None)(sc: SparkContext): RDD[Event] = {

    checkTableExists(appId, channelId)

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,
      HBEventsUtil.tableName(namespace, appId, channelId))

    val scan = HBEventsUtil.createScan(
      startTime = startTime,
      untilTime = untilTime,
      entityType = entityType,
      entityId = entityId,
      eventNames = eventNames,
      targetEntityType = targetEntityType,
      targetEntityId = targetEntityId,
      reversed = None
    )
    scan.setCaching(500)
    scan.setCacheBlocks(false)

    conf.set(TableInputFormat.SCAN, MBAHBaseUtil.convertScanToString(scan))

    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map {
      case (key, row) => HBEventsUtil.resultToEvent(row, appId)
    }
    rdd
  }

  override
  def write(
           events: RDD[Event], appId: Int, channelId: Option[Int]
             )(sc: SparkContext): Unit = {

    checkTableExists(appId, channelId)

    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE,
      HBEventsUtil.tableName(namespace, appId, channelId))
    conf.setClass("mapreduce.outputformat.class",
      classOf[TableOutputFormat[Object]],
      classOf[OutputFormat[Object, Writable]])

    events.map { event =>
      val (put, rowKey) = HBEventsUtil.eventToPut(event, appId)
      (new ImmutableBytesWritable(rowKey.toBytes), put)
    }.saveAsNewAPIHadoopDataset(conf)
  }

}
