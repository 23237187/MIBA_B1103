package zte.MBA.data.storage.elasticsearch

import grizzled.slf4j.Logging
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.FilterBuilders._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import zte.MBA.data.storage.{StorageClientConfig, Apps, App}
import org.json4s.native.Serialization.read
import org.json4s.native.Serialization.write



class ESApps(client: Client, config: StorageClientConfig, index: String)
  extends Apps with Logging {
  implicit val formats = DefaultFormats.lossless
  private val estype = "apps"
  private val seq = new ESSequences(client, config, index)

  val indices = client.admin.indices()
  val indexExistResponse = indices.prepareExists(index).get()
  if (!indexExistResponse.isExists) {
    indices.prepareCreate(index).get()
  }

  val typeExistResponse = indices.prepareTypesExists(index).setTypes(estype).get()
  if (!typeExistResponse.isExists) {
    val json =
      (estype ->
        ("properties" ->
          (("name" -> ("type" -> "string")) ~
          ("index" -> "not_analyzed")
            )))
    indices.preparePutMapping(index).setType(estype).setSource(compact(render(json))).get()
  }

  def insert(app: App): Option[Int] = {
    val id =
      if (app.id == 0) {
        var roll = seq.genNext("apps")
        while (!get(roll).isEmpty) roll = seq.genNext("apps")
        roll
      } else {
        app.id
      }
    val realapp = app.copy(id = id)
    if (update(realapp)) Some(id) else None
  }

  def get(id: Int): Option[App] = {
    try {
      val response = client.prepareGet(
        index,
        estype,
        id.toString).get()
      Some(read[App](response.getSourceAsString))
    } catch {
      case e: ElasticsearchException =>
        error(e.getMessage)
        None
      case e: NullPointerException => None
    }
  }

  def getByName(name: String): Option[App] = {
    try {
      val response = client.prepareSearch(index).setTypes(estype).
        setPostFilter(termFilter("name", name)).get
      val hits = response.getHits().hits()
      if (hits.size > 0) {
        Some(read[App](hits.head.getSourceAsString))
      } else {
        None
      }
    } catch {
      case e: ElasticsearchException =>
        error(e.getMessage)
        None
    }
  }

  def getAll(): Seq[App] = {
    try {
      val builder = client.prepareSearch(index).setTypes(estype)
      ESUtils.getAll[App](client, builder)
    } catch {
      case e: ElasticsearchException =>
        error(e.getMessage)
        Seq[App]()
    }
  }

  def update(app: App): Boolean = {
    try {
      val response = client.prepareIndex(index, estype, app.id.toString).
        setSource(write(app)).get()
      true
    } catch {
      case e: ElasticsearchException =>
        error(e.getMessage)
        false
    }
  }

  def delete(id: Int): Boolean = {
    try {
      client.prepareDelete(index, estype, id.toString).get
      true
    } catch {
      case e: ElasticsearchException =>
        error(e.getMessage)
        false
    }
  }
}
