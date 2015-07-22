package zte.MBA.data.storage.hbase

import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{ZooKeeperConnectionException, MasterNotRunningException, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HConnectionManager, HBaseAdmin, HConnection}
import zte.MBA.data.storage.{BaseStorageClient, StorageClientConfig}


case class HBClient(
                   val conf: Configuration,
                   val connection: HConnection,
                   val admin: HBaseAdmin
                     )

class StorageClient(val config: StorageClientConfig)
  extends BaseStorageClient with Logging {

  val conf = HBaseConfiguration.create()

  if (config.test) {
    conf.set("hbase.client.retries.number", "1")
    conf.set("zookeeper.session.timeout", "30000")
    conf.set("zookeeper.recovery.retry", "1")
  }

  try {
    HBaseAdmin.checkHBaseAvailable(conf)
  } catch {
    case e: MasterNotRunningException =>
      error("HBase master is not running (ZooKeeper ensemble: " +
        conf.get("hbase.zookeeper.quorum") + "). Please make sure that HBase " +
        "is running properly, and that the configuration is pointing at the " +
        "correct ZooKeeper ensemble.")
      throw e
    case e: ZooKeeperConnectionException =>
      error("Cannot connect to ZooKeeper (ZooKeeper ensemble: " +
        conf.get("hbase.zookeeper.quorum") + "). Please make sure that the " +
        "configuration is pointing at the correct ZooKeeper ensemble. By " +
        "default, HBase manages its own ZooKeeper, so if you have not " +
        "configured HBase to use an external ZooKeeper, that means your " +
        "HBase is not started or configured properly.")
      throw e
    case e: Exception => {
      error("Failed to connect to HBase." +
        " Please check if HBase is running properly.")
      throw e
    }
  }

  val connection = HConnectionManager.createConnection(conf)

  val client = HBClient(
    conf = conf,
    connection = connection,
    admin = new HBaseAdmin(connection)
  )

  override
  val prefix = "HB"
}
