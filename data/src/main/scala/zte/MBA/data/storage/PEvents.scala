package zte.MBA.data.storage

import grizzled.slf4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.reflect.ClassTag


trait PEvents extends Serializable {
  @transient protected lazy val logger = Logger[this.type]
  def getByAppIdAndTimeAndEntity(
                                appId: Int,
                                startTime: Option[DateTime],
                                untilTime: Option[DateTime],
                                entityType: Option[String],
                                entityId: Option[String]
                                  )(sc: SparkContext): RDD[Event] = {
    find(
      appId = appId,
      startTime = startTime,
      untilTime = untilTime,
      entityType = entityType,
      entityId = entityId,
      eventNames = None
    )(sc)
  }

  def find(
          appId: Int,
          channelId: Option[Int] = None,
          startTime: Option[DateTime] = None,
          untilTime: Option[DateTime] = None,
          entityType: Option[String] = None,
          entityId: Option[String] = None,
          eventNames: Option[Seq[String]] = None,
          targetEntityType: Option[Option[String]] = None,
          targetEntityId: Option[Option[String]] = None
            )(sc: SparkContext): RDD[Event]

  def aggregateProperties(
                         appId: Int,
                         channelId: Option[Int] = None,
                         entityType: String,
                         startTime: Option[DateTime] = None,
                         untilTime: Option[DateTime] = None,
                         required: Option[Seq[String]] = None
                           )(sc: SparkContext): RDD[(String, PropertyMap)] = {
    val eventRDD = find(
      appId = appId,
      channelId = channelId,
      startTime = startTime,
      untilTime = untilTime,
      entityType = Some(entityType),
      eventNames = Some(PEventAggregator.eventNames)
    )(sc)

    val dmRDD = PEventAggregator.aggregateProperties(eventRDD)

    required map { r =>
      dmRDD.filter { case (k, v) =>
        r.map(v.contains(_)).reduce(_&&_)
      }
    } getOrElse dmRDD
  }

  def extractEntityMap[A: ClassTag](
                                   appId: Int,
                                   entityType: String,
                                   startTime: Option[DateTime] = None,
                                   untilTime: Option[DateTime] = None,
                                   required: Option[Seq[String]] = None
                                     )(sc: SparkContext)(extract: DataMap => A): EntityMap[A] = {
    val idToData: Map[String, A] = aggregateProperties(
      appId = appId,
      entityType = entityType,
      startTime = startTime,
      untilTime = untilTime,
      required = required
    )(sc).map { case (id, dm) =>
      try {
        (id, extract(dm))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get extract entity from DataMap $dm of " +
            s"entityId $id.", e)
          throw e
        }
      }
    }.collectAsMap.toMap

    new EntityMap(idToData)
  }

  def write(events: RDD[Event], appId: Int)(sc: SparkContext): Unit = {
    write(events, appId, None)(sc)
  }

  def write(events: RDD[Event], appId: Int, channelId: Option[Int])(sc: SparkContext): Unit
}
