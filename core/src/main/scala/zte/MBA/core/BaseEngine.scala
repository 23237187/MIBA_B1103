package zte.MBA.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.JsonAST.JValue
import zte.MBA.controller.EngineParams
import zte.MBA.workflow.JsonExtractorOption.JsonExtractorOption
import zte.MBA.workflow.WorkflowParams


abstract class BaseEngine[EI, Q, P, A] extends Serializable {

  def train(
    sc: SparkContext,
    engineParams: EngineParams,
    engineInstanceId: String,
    params: WorkflowParams): Seq[Any]

  def eval(
    sc: SparkContext,
    engineParams: EngineParams,
    params: WorkflowParams): Seq[(EI, RDD[(Q, P, A)])]

  def batchEval(
    sc: SparkContext,
    engineParamsList: Seq[EngineParams],
    params: WorkflowParams): Seq[(EngineParams, Seq[(EI, RDD[(Q, P, A)])])] = {

    engineParamsList.map { engineParams =>
      (engineParams, eval(sc, engineParams, params))
    }
  }

  def jValueToEngineParams(variantJson: JValue, jsonExtractor: JsonExtractorOption): EngineParams = {
    throw new NotImplementedError("JSON to EngineParams is not implemented.")
  }

}
