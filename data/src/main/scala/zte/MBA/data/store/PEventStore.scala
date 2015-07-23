package zte.MBA.data.store

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import zte.MBA.data.storage.{Event, Storage}

object PEventStore {
  @transient lazy private val eventsDb = Storage.getPEvents()

  def find(
          appName: String,
          channelName: Option[String] = None,
          startTime: Option[DateTime] = None,
          untilTime: Option[DateTime] = None,
          entityType: Option[String] = None,
          entityId: Option[String] = None,
          eventNames: Option[Seq[String]] = None,
          targetEntityType: Option[Option[String]] = None,
          targetEntityId: Option[Option[String]] = None
            )(sc: SparkContext): RDD[Event] = {
    val (appId, channelId) = Common
  }
}
