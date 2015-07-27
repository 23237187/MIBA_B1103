package zte.MBA.data.storage.elasticsearch

import grizzled.slf4j.Logging
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.client.Client
import org.json4s.DefaultFormats
import org.json4s.native.Serialization._
import zte.MBA.data.storage.{EngineManifest, EngineManifestSerializer, EngineManifests, StorageClientConfig}


class ESEngineManifests(client: Client, config: StorageClientConfig, index: String)
  extends EngineManifests with Logging {
  implicit val formats = DefaultFormats + new EngineManifestSerializer
  private val estype = "engine_manifests"
  private def esid(id: String, version: String) = s"$id $version"

  def insert(engineManifest: EngineManifest): Unit = {
    val json = write(engineManifest)
    val response = client.prepareIndex(
      index,
      estype,
      esid(engineManifest.id, engineManifest.version)).
      setSource(json).execute().actionGet()
  }

  def get(id: String, version: String): Option[EngineManifest] = {
    try {
      val response = client.prepareGet(index, estype, esid(id, version)).
        execute().actionGet()
      if (response.isExists) {
        Some(read[EngineManifest](response.getSourceAsString))
      } else {
        None
      }
    } catch {
      case e: ElasticsearchException =>
        error(e.getMessage)
        None
    }
  }

  def getAll(): Seq[EngineManifest] = {
    try {
      val builder = client.prepareSearch()
      ESUtils.getAll[EngineManifest](client, builder)
    } catch {
      case e: ElasticsearchException =>
        error(e.getMessage)
        Seq()
    }
  }

  def update(engineManifest: EngineManifest, upsert: Boolean = false): Unit =
    insert(engineManifest)

  def delete(id: String, version: String): Unit = {
    try {
      client.prepareDelete(index, estype, esid(id, version)).execute().actionGet()
    } catch {
      case e: ElasticsearchException => error(e.getMessage)
    }
  }
}