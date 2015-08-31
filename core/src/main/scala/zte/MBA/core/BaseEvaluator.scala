package zte.MBA.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import zte.MBA.controller.{Evaluation, EngineParams}
import zte.MBA.workflow.WorkflowParams


abstract class BaseEvaluator[EI, Q, P, A, ER <: BaseEvaluatorResult]
  extends AbstractDoer {

  def evaluateBase(
      sc: SparkContext,
      evaluation: Evaluation,
      engineEvalDataSet: Seq[(EngineParams, Seq[(EI, RDD[(Q, P, A)])])],
      params: WorkflowParams): ER
}

trait BaseEvaluatorResult extends Serializable {
  def toOneLiner(): String = ""

  def toHTML(): String = ""

  def toJSON(): String = ""

  val noSave: Boolean = false
}
