package zte.MBA.data.storage.elasticsearch

import grizzled.slf4j.Logging
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.client.Client
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import zte.MBA.data.storage.StorageClientConfig

class ESSequences(client: Client, config: StorageClientConfig, index: String) extends Logging{
  implicit val formats = DefaultFormats
  private val estype = "sequences"

  val indices = client.admin.indices()
  val indexExistResponse = indices.prepareExists(index).get()
  if (!indexExistResponse.isExists) {
    indices.prepareCreate(index).get()
  }

  val typeExistResponse = indices.prepareTypesExists(index).setTypes(estype).get()
  if (!typeExistResponse.isExists) {
    val mappingJson =
      (estype ->
        ("_source" -> ("enabled" -> 0)) ~
        ("_all" -> ("enabled" -> 0)) ~
        ("_type" -> ("index" -> "no")) ~
        ("enabled" -> 0))
    indices.preparePutMapping(index).setType(estype).setSource(compact(render(mappingJson))).get()
  }

  def genNext(name: String): Int = {
    try {
      val response = client.prepareIndex(index, estype, name).setSource(compact(render("n" -> name))).get
      response.getVersion().toInt
    } catch {
      case e: ElasticsearchException =>
        error(e.getMessage)
        0
    }
  }
}
