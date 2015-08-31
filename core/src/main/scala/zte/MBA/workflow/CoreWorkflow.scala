package zte.MBA.workflow

import grizzled.slf4j.Logger
import zte.MBA.controller.{Evaluation, EngineParams}
import zte.MBA.core.{BaseEvaluator, BaseEvaluatorResult, BaseEngine}
import zte.MBA.data.storage.{EvaluationInstance, Model, EngineInstance, Storage}

import com.github.nscala_time.time.Imports.DateTime


object CoreWorkflow {

  @transient lazy val logger = Logger[this.type]
  @transient lazy val engineInstances = Storage.getMetaDataEngineInstances()
  @transient lazy val evaluationInstances =
    Storage.getMetaDataEvaluationInstances()

  def runTrain[EI, Q, P, A](
    engine: BaseEngine[EI, Q, P, A],
    engineParams: EngineParams,
    engineInstance: EngineInstance,
    env: Map[String, String] = WorkflowUtils.mbaEnvVars,
    params: WorkflowParams = WorkflowParams()): Unit = {

    logger.debug("Starting SparkContext")
    val mode = "training"
    WorkflowUtils.checkUpgrade(mode, engineInstance.engineFactory)

    var sc = WorkflowContext(
      params.batch,
      env,
      params.sparkEnv,
      mode.capitalize)

    try {
      val models: Seq[Any] = engine.train(
        sc = sc,
        engineParams = engineParams,
        engineInstanceId = engineInstance.id,
        params = params
      )

      val instanceId = Storage.getMetaDataEngineInstances()

      val kryo = KryoInstantiator.newKryoInjection

      logger.info("Inserting persistent model")
      Storage.getModelDataModels.insert(Model(
        id = engineInstance.id,
        models = kryo(models)
      ))

      logger.info("Updating engine instance")
      val engineInstances = Storage.getMetaDataEngineInstances
      engineInstances.update(engineInstance.copy(
        status = "COMPLETED",
        endTime = DateTime.now
      ))

      logger.info("Training completed successfully.")
    } catch {
      case e @(
          _: StopAfterReadInterruption |
          _: StopAfterPrepareInterruption) => {
        logger.info(s"Training interrupted by $e.")
      }
    } finally {
      logger.debug("Stopping SparkContext")
      sc.stop()
    }
  }

  def runEvaluation[EI, Q, P, A, R <: BaseEvaluatorResult](
    evaluation: Evaluation,
    engine: BaseEngine[EI, Q, P, A],
    engineParamsList: Seq[EngineParams],
    evaluationInstance: EvaluationInstance,
    evaluator: BaseEvaluator[EI, Q, P, A, R],
    env: Map[String, String] = WorkflowUtils.mbaEnvVars,
    params: WorkflowParams = WorkflowParams()): Unit ={


    logger.info("runEvaluation started")
    logger.debug("Start SparkContext")

    val mode = "evaluation"

    WorkflowUtils.checkUpgrade(mode, engine.getClass.getName)

    val sc = WorkflowContext(
      params.batch,
      env,
      params.sparkEnv,
      mode.capitalize)
    val evaluationInstanceId = evaluationInstances.insert(evaluationInstance)

    logger.info(s"Starting evaluation instance ID: $evaluationInstanceId")

    val evaluatorResult: BaseEvaluatorResult = EvaluationWorkflow.runEvaluation(
      sc,
      evaluation,
      engine,
      engineParamsList,
      evaluator,
      params)

    if (evaluatorResult.noSave) {
      logger.info(s"This evaluation result is not inserted into database: $evaluatorResult")
    } else {
      val evaluatedEvaluationInstance = evaluationInstance.copy(
        status = "EVALCOMPLETED",
        id = evaluationInstanceId,
        endTime = DateTime.now,
        evaluatorResults = evaluatorResult.toOneLiner,
        evaluatorResultsHTML = evaluatorResult.toHTML,
        evaluatorResultsJSON = evaluatorResult.toJSON
      )

      logger.info(s"Updating evaluation instance with result: $evaluatorResult")

      evaluationInstances.update(evaluationInstance)
    }

    logger.debug("Stop SparkContext")

    sc.stop()

    logger.info("runEvaluation completed")
  }




}
