package zte.MBA.data.storage

import grizzled.slf4j.Logger
import org.joda.time.DateTime


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
}
