package zte.MBA.data.storage.localfs

import java.io.File

import grizzled.slf4j.Logging
import zte.MBA.data.storage.{StorageClientException, BaseStorageClient, StorageClientConfig}


class StorageClient(val config: StorageClientConfig) extends BaseStorageClient
  with Logging {
  override val prefix = "LocalFS"
  val f = new File(
    config.properties.getOrElse("PATH", config.properties("HOSTS"))
  )
  if (f.exists) {
    if (!f.isDirectory) throw new StorageClientException(
      s"${f} already exists but it is not a directory!",
      null
    )
    if (!f.canWrite) throw new StorageClientException(
      s"${f} already exists but it is not writable!",
      null)
  } else {
    if (!f.mkdirs) throw new StorageClientException(
      s"${f} does not exist and automatic creation failed!",
      null)
  }
  val client = f
}
