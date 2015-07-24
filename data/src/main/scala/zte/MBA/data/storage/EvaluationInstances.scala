package zte.MBA.data.storage

import org.joda.time.DateTime
import org.json4s.{Extraction, DefaultFormats, CustomSerializer}
import org.json4s.JsonAST.{JField, JString, JObject}

case class EvaluationInstance(
  id: String = "",
  status: String = "",
  startTime: DateTime = DateTime.now,
  endTime: DateTime = DateTime.now,
  evaluationClass: String = "",
  engineParamsGeneratorClass: String = "",
  batch: String = "",
  env: Map[String, String] = Map(),
  sparkConf: Map[String, String] = Map(),
  evaluatorResults: String = "",
  evaluatorResultsHTML: String = "",
  evaluatorResultsJSON: String = ""
  )


trait EvaluationInstances {
  def insert(i: EvaluationInstance): String

  def get(id: String): Option[EvaluationInstance]

  def getAll: Seq[EvaluationInstance]

  def getCompleted: Seq[EvaluationInstance]

  def update(i: EvaluationInstance): Unit

  def delete(id: String): Unit
}


class EvaluationInstanceSerializer extends CustomSerializer[EvaluationInstance] (
  format => ({
    case JObject(fields) =>
      implicit val formats = DefaultFormats
      fields.foldLeft(EvaluationInstance()) { case (i, field) =>
        field match {
          case JField("id", JString(id)) => i.copy(id = id)
          case JField("status", JString(status)) => i.copy(status = status)
          case JField("startTime", JString(startTime)) =>
            i.copy(startTime = Utils.stringToDateTime(startTime))
          case JField("endTime", JString(endTime)) =>
            i.copy(endTime = Utils.stringToDateTime(endTime))
          case JField("evaluationClass", JString(evaluationClass)) =>
            i.copy(evaluationClass = evaluationClass)
          case JField("engineParamsGeneratorClass", JString(engineParamsGeneratorClass)) =>
            i.copy(engineParamsGeneratorClass = engineParamsGeneratorClass)
          case JField("batch", JString(batch)) => i.copy(batch = batch)
          case JField("env", env) =>
            i.copy(env = Extraction.extract[Map[String, String]](env))
          case JField("sparkConf", sparkConf) =>
            i.copy(sparkConf = Extraction.extract[Map[String, String]](sparkConf))
          case JField("evaluatorResults", JString(evaluatorResults)) =>
            i.copy(evaluatorResults = evaluatorResults)
          case JField("evaluatorResultsHTML", JString(evaluatorResultsHTML)) =>
            i.copy(evaluatorResultsHTML = evaluatorResultsHTML)
          case JField("evaluatorResultsJSON", JString(evaluatorResultsJSON)) =>
            i.copy(evaluatorResultsJSON = evaluatorResultsJSON)
          case _ => i
        }
      }
  }, {
    case i: EvaluationInstance =>
      JObject(
        JField("id", JString(i.id)) ::
          JField("status", JString(i.status)) ::
          JField("startTime", JString(i.startTime.toString)) ::
          JField("endTime", JString(i.endTime.toString)) ::
          JField("evaluationClass", JString(i.evaluationClass)) ::
          JField("engineParamsGeneratorClass", JString(i.engineParamsGeneratorClass)) ::
          JField("batch", JString(i.batch)) ::
          JField("env", Extraction.decompose(i.env)(DefaultFormats)) ::
          JField("sparkConf", Extraction.decompose(i.sparkConf)(DefaultFormats)) ::
          JField("evaluatorResults", JString(i.evaluatorResults)) ::
          JField("evaluatorResultsHTML", JString(i.evaluatorResultsHTML)) ::
          JField("evaluatorResultsJSON", JString(i.evaluatorResultsJSON)) ::
          Nil
      )
  }))

