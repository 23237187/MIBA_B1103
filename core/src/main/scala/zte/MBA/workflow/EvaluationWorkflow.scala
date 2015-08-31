package zte.MBA.workflow

import org.apache.spark.SparkContext
import org.slf4j.Logger
import zte.MBA.controller.{EngineParams, Evaluation}
import zte.MBA.core.{BaseEvaluator, BaseEngine, BaseEvaluatorResult}


object EvaluationWorkflow {
  @transient lazy val logger = Logger[this.type]
  def runEvaluation[EI, Q, P, A, R <: BaseEvaluatorResult](
    sc: SparkContext,
    evaluation: Evaluation,
    engine: BaseEngine[EI, Q, P, A],
    engineParamsList: Seq[EngineParams],
    evaluator: BaseEvaluator[EI, Q, P, A, R],
    params: WorkflowParams): R = {

    val engineEvalDataSet = engine.batchEval(sc, engineParamsList, params)
    evaluator.evaluateBase(sc, evaluation, engineEvalDataSet, params)
  }
}
