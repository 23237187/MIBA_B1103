package zte.MBA.data.storage

import org.joda.time.DateTime
import org.json4s.{Extraction, DefaultFormats, CustomSerializer}
import org.json4s.JsonAST.{JString, JField, JObject}
import zte.MBA.data.storage.Utils


case class EngineInstance(
  id: String,
  status: String,
  startTime: DateTime,
  endTime: DateTime,
  engineId: String,
  engineVersion: String,
  engineVariant: String,
  engineFactory: String,
  batch: String,
  env: Map[String, String],
  sparkConf: Map[String, String],
  dataSourceParams: String,
  preparatorParams: String,
  algorithmsParams: String,
  servingParams: String)

trait EngineInstances {
  def insert(i: EngineInstance): String

  def get(id: String): Option[EngineInstance]

  def getAll(): Seq[EngineInstance]

  def getLatestCompleted(
    engineId: String,
    engineVersion: String,
    engineVariant: String): Option[EngineInstance]

  def getCompleted(
    engineId: String,
    engineVersion: String,
    engineVariant: String): Option[EngineInstance]

  def update(i: EngineInstance): Unit

  def delete(id: String): Unit
}

class EngineInstanceSerializer
  extends CustomSerializer[EngineInstance](
    format => ({
      case JObject(fields) =>
        implicit val formats = DefaultFormats
        val seed = EngineInstance(
          id = "",
          status = "",
          startTime = DateTime.now,
          endTime = DateTime.now,
          engineId = "",
          engineVersion = "",
          engineVariant = "",
          engineFactory = "",
          batch = "",
          env = Map(),
          sparkConf = Map(),
          dataSourceParams = "",
          preparatorParams = "",
          algorithmsParams = "",
          servingParams = "")
        fields.foldLeft(seed) { case (i, field) =>
          field match {
            case JField("id", JString(id)) => i.copy(id = id)
            case JField("status", JString(status)) => i.copy(status = status)
            case JField("startTime", JString(startTime)) =>
              i.copy(startTime = Utils.stringToDateTime(startTime))
            case JField("endTime", JString(endTime)) =>
              i.copy(endTime = Utils.stringToDateTime(endTime))
            case JField("engineId", JString(engineId)) =>
              i.copy(engineId = engineId)
            case JField("engineVersion", JString(engineVersion)) =>
              i.copy(engineVersion = engineVersion)
            case JField("engineVariant", JString(engineVariant)) =>
              i.copy(engineVariant = engineVariant)
            case JField("engineFactory", JString(engineFactory)) =>
              i.copy(engineFactory = engineFactory)
            case JField("batch", JString(batch)) => i.copy(batch = batch)
            case JField("env", env) =>
              i.copy(env = Extraction.extract[Map[String, String]](env))
            case JField("sparkConf", sparkConf) =>
              i.copy(sparkConf = Extraction.extract[Map[String, String]](sparkConf))
            case JField("dataSourceParams", JString(dataSourceParams)) =>
              i.copy(dataSourceParams = dataSourceParams)
            case JField("preparatorParams", JString(preparatorParams)) =>
              i.copy(preparatorParams = preparatorParams)
            case JField("algorithmsParams", JString(algorithmsParams)) =>
              i.copy(algorithmsParams = algorithmsParams)
            case JField("servingParams", JString(servingParams)) =>
              i.copy(servingParams = servingParams)
            case _ => i
          }
        }
    },
      {
        case i: EngineInstance =>
          JObject(
            JField("id", JString(i.id)) ::
              JField("status", JString(i.status)) ::
              JField("startTime", JString(i.startTime.toString)) ::
              JField("endTime", JString(i.endTime.toString)) ::
              JField("engineId", JString(i.engineId)) ::
              JField("engineVersion", JString(i.engineVersion)) ::
              JField("engineVariant", JString(i.engineVariant)) ::
              JField("engineFactory", JString(i.engineFactory)) ::
              JField("batch", JString(i.batch)) ::
              JField("env", Extraction.decompose(i.env)(DefaultFormats)) ::
              JField("sparkConf", Extraction.decompose(i.sparkConf)(DefaultFormats)) ::
              JField("dataSourceParams", JString(i.dataSourceParams)) ::
              JField("preparatorParams", JString(i.preparatorParams)) ::
              JField("algorithmsParams", JString(i.algorithmsParams)) ::
              JField("servingParams", JString(i.servingParams)) ::
              Nil)
      }
      ))
