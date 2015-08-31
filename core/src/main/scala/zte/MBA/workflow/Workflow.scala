package zte.MBA.workflow

import zte.MBA.controller.EngineParams
import zte.MBA.core.BaseEngine
import zte.MBA.data.storage.EvaluationInstance


object Workflow {

  def runEvaluation(
    evaluation: Evaluation,
    engineParamsGenerator: EngineParamsGenerator,
    env: Map[String, String] = WorkflowUtils.mbaEnvVars,
    evaluationInstance: EvaluationInstance = EvaluationInstance(),
    params: WorkflowParams = WorkflowParams()){

    runEvaluationTypeless(
      evaluation = evaluation,
      engine = evaluation.engine,
      engineParamsList = engineParamsGenerator.engineParamsList,
      evaluationInstance = evaluationInstance,
      evaluator = evaluation.evaluator,
      env = env,
      params = params
    )
  }

  def runEvaluationTypeless[
      EI, Q, P, A, EEI, EQ, EP, EA, ER <: BaseEvaluatorResult](
      evaluation: Evaluation,
      engine: BaseEngine[EI, Q, P, A],
      engineParamsList: Seq[EngineParams],
      evaluationInstance: EvaluationInstance,
      evaluator: BaseEvaluator[EEI, EQ, EP, EA, ER],
      env: Map[String, String] = WorkflowUtils.mbaEnvVars,
      params: WorkflowParams = WorkflowParams()): Unit ={

      runEvaluation(
        evaluation = evaluation,
        engine = engine,
        engineParamsList = engineParamsList,
        evaluationInstance = evaluationInstance,
        evaluator = evaluator.asInstanceOf[BaseEvaluator[EI, Q, P, A, ER]],
        env = env,
        params = params)
  }

  def runEvaluatio[EI, Q, P, A, R <: BaseEvaluatorResult](
      evaluation: Evaluation,
      engine: BaseEngine[EI, Q, P, A],
      engineParamsList: Seq[EngineParams],
      evaluationInstance: EvaluationInstance,
      evaluator: BaseEvaluator[EI, Q, P, A, R],
      env: Map[String, String] = WorkflowUtils.mbaEnvVars,
      params: WorkflowParams = WorkflowParams()): Unit ={

    CoreWorkflow.runEvaluation(
      evaluation = evaluation,
      engine = engine,
      engineParamsList = engineParamsList,
      evaluationInstance = evaluationInstance,
      evaluator = evaluator,
      env = env,
      params = params
    )
  }


}
