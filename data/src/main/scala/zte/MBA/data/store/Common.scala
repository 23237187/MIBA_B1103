package zte.MBA.data.store

import grizzled.slf4j.Logger
import zte.MBA.data.storage.Storage


private[MBA] object Common {
  @transient lazy val logger = Logger[this.type]
  @transient lazy private val appsDb = Storage.getMetaDataApps()
  @transient lazy private val channelsDb = Storage.getMetaDataChannels()

  def appNameToId(appName: String, channelName: Option[String]): (Int, Option[Int]) = {
    val appOpt = appsDb.getByName(appName)

    appOpt.map { app =>
      val channelMap: Map[String, Int] = channelsDb.getByAppid(app.id)
        .map(c => (c.name, c.id)).toMap

      val channelId: Option[Int] = channelName.map { ch =>
        if (channelMap.contains(ch)) {
          channelMap(ch)
        } else {
          logger.error(s"Invalid channel name ${ch}.")
          throw new IllegalArgumentException(s"Invalid channel name ${ch}.")
        }
      }

      (app.id, channelId)
    }.getOrElse {
      logger.error(s"Invalid app name ${appName}")
      throw new IllegalArgumentException(s"Invalid app name ${appName}")
    }
  }
}
