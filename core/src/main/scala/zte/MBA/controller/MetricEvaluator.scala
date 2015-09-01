package zte.MBA.controller

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hamcrest.Description
import grizzled.slf4j.Logger
import zte.MBA.controller.MetricEvaluator.NameParams
import zte.MBA.core.{BaseEvaluator, BaseEvaluatorResult}
import zte.MBA.workflow.{WorkflowParams, NameParamsSerializer}
import zte.MBA.data.storage.Storage
import org.json4s.native.Serialization.write
import org.json4s.native.Serialization.writePretty
import com.github.nscala_time.time.Imports.DateTime

case class MetricScores[R](
  score: R,
  otherScores: Seq[Any])

case class MetricEvaluatorResult[R](
  bestScore: MetricScores[R],
  bestEngineParams: EngineParams,
  bestIdx: Int,
  metricHeader: String,
  otherMetricHeaders: Seq[String],
  engineParamsScores: Seq[(EngineParams, MetricScores[R])],
  outputPath: Option[String])
  extends BaseEvaluatorResult {

  override
  def toOneLiner(): String = {
    val idx = engineParamsScores.map(_._1).indexOf(bestEngineParams)
    s"Best Params Index: $idx Score: ${bestScore.score}"
  }

  override
  def toJSON(): String = {
    implicit lazy val formats = Utils.json4sDefaultFormats +
      new NameParamsSerializer
    write(this)
  }

  override
  def toHTML(): String = html.metric_evaluator().toString

  override
  def toString: String = {
    implicit lazy val formats = Utils.json4sDefaultFormats +
      new NameParamsSerializer

    val bestEPStr = writePretty(bestEngineParams)

    val strings = (
      Seq(
        "MetricEvaluatorResult:",
        s"  # engine params evaluated: ${engineParamsScores.size}"
      ) ++
      Seq(
        "Optimal Engine Params:",
        s"  $bestEPStr",
        "Metrics:",
        s"  $metricHeader: ${bestScore.score}"
      ) ++
      otherMetricHeaders.zip(bestScore.otherScores).map {
        case (h, s) => s"  $h: $s"
      } ++
      outputPath.toSeq.map {
        p => s"The best variant params can be found in $p"
      })

    strings.mkString("\n")
  }
}

object MetricEvaluator {
  def apply[EI, Q, P, A, R](
    metric: Metric[EI, Q, P, A, R],
    otherMetrics: Seq[Metric[EI, Q, P, A, _]],
    outputPath: String): MetricEvaluator[EI, Q, P, A, R] = {

    new MetricEvaluator[EI, Q, P, A, R](
      metric,
      otherMetrics,
      Some(outputPath))
  }

  def apply[EI, Q, P, A, R](
    metric: Metric[EI, Q, P, A, R],
    otherMetrics: Seq[Metric[EI, Q, P, A, _]])
    : MetricEvaluator[EI, Q, P, A, R] = {
    new MetricEvaluator[EI, Q, P, A, R](
      metric,
      otherMetrics,
      None)
  }

  def apply[EI, Q, P, A, R](metric: Metric[EI, Q, P, A, R])
  : MetricEvaluator[EI, Q, P, A, R] = {
    new MetricEvaluator[EI, Q, P, A, R](
      metric,
      Seq[Metric[EI, Q, P, A, _]](),
      None)
  }

  case class NameParams(name: String, params: Params) {
    def this(np: (String, Params)) = this(np._1, np._2)
  }

  case class EngineVariant(
    id: String,
    description: String,
    engineFactory: String,
    dataSource: NameParams,
    preparator: NameParams,
    algorithms: Seq[NameParams],
    serving: NameParams) {

    def this(evaluation: Evaluation, engineParams: EngineParams) = this(
      id = "",
      description = "",
      engineFactory = evaluation.getClass.getName,
      dataSource = new NameParams(engineParams.dataSourceParams),
      preparator = new NameParams(engineParams.preparatorParams),
      algorithms = engineParams.algorithmParamsList.map(np => new NameParams(np)),
      serving = new NameParams(engineParams.servingParams))
  }
}

private [MBA]
class MetricEvaluator[EI, Q, P, A, R] (
  val metric: Metric[EI, Q, P, A, R],
  val otherMetircs: Seq[Metric[EI, Q, P, A, _]],
  val outputPath: Option[String])
  extends BaseEvaluator[EI, Q, P, A, MetricEvaluatorResult[R]] {

  @transient lazy val logger = Logger[this.type]
  @transient lazy val engineInstances = Storage.getMetaDataEngineInstances()

  def saveEngineJson(
    evaluation: Evaluation,
    engineParams: EngineParams,
    outputPath: String): Unit = {

    val now = DateTime.now
    val evalClassName = evaluation.getClass.getName

    val variant = MetricEvaluator.EngineVariant(
      id = s"$evalClassName $now",
      description = "",
      engineFactory = evalClassName,
      dataSource = new MetricEvaluator.NameParams(engineParams.dataSourceParams),
      preparator = new MetricEvaluator.NameParams(engineParams.preparatorParams),
      algorithms = engineParams.algorithmParamsList.map(np => new MetricEvaluator.NameParams(np)),
      serving = new MetricEvaluator.NameParams(engineParams.servingParams))

    implicit lazy val formats = Utils.json4sDefaultFormats

    logger.info(s"Writing best variant params to disk ($outputPath)...")
    val writer = new PrintWriter(new File(outputPath))
    writer.write(writePretty(variant))
    writer.close()
  }

  def evaluateBase(
    sc: SparkContext,
    evaluation: Evaluation,
    engineEvalDataSet: Seq[(EngineParams, Seq[(EI, RDD[(Q, P, A)])])],
    params: WorkflowParams): MetricEvaluatorResult[R] = {

    val evalResultList: Seq[(EngineParams, MetricScores[R])] = engineEvalDataSet
      .zipWithIndex
      .par
      .map { case ((engineParams, evalDataSet), idx) =>
        val metricScores = MetricScores[R](
          metric.calculate(sc, evalDataSet),
          otherMetircs.map(_.calculate(sc, evalDataSet)))
      (engineParams, metricScores)
      }
      .seq

    implicit lazy val formats = Utils.json4sDefaultFormats +
      new NameParamsSerializer

    evalResultList.zipWithIndex.foreach { case ((ep, r), idx) => {
      logger.info(s"Iteration $idx")
      logger.info(s"EngineParams: ${write(ep)}")
      logger.info(s"Result: $r")
    }}

    val ((bestEngineParams, bestScore), bestIdx) = evalResultList
      .zipWithIndex
      .reduce { (x, y) =>
        (if (metric.compare(x._1._2.score, y._1._2.score) >= 0) x else y)
    }

    outputPath.foreach {path => saveEngineJson(evaluation, bestEngineParams, path)}

    MetricEvaluatorResult(
      bestScore = bestScore,
      bestEngineParams = bestEngineParams,
      bestIdx = bestIdx,
      metricHeader = metric.header,
      otherMetricHeaders = otherMetircs.map(_.header),
      engineParamsScores = evalResultList,
      outputPath = outputPath)

  }
}
