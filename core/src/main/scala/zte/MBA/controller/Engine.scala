package zte.MBA.controller

import grizzled.slf4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.JsonAST.{JArray, JField, JValue}
import zte.MBA.core._
import zte.MBA.data.storage.StorageClientException
import zte.MBA.workflow.JsonExtractorOption.JsonExtractorOption
import zte.MBA.workflow._
import zte.MBA.workflow.CreateWorkflow
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.read

import scala.collection.JavaConversions


class Engine[TD, EI, PD, Q, P, A](
  val dataSourceClassMap: Map[String,
    Class[_ <: BaseDataSource[TD, EI, Q, A]]],
  val preparatorClassMap: Map[String, Class[_ <: BasePreparator[TD, PD]]],
  val algorithmClassMap: Map[String, Class[_ <: BaseAlgorithm[PD, _, Q, P]]],
  val servingClassMap: Map[String, Class[_ <: BaseServing[Q, P]]])
  extends BaseEngine[EI, Q, P, A] {

  private[MBA]
  implicit lazy val formats = Utils.json4sDefaultFormats + new NameParamsSerializer

  @transient lazy protected val logger = Logger[this.type]

  def this(
    dataSourceClass: Class[_ <: BaseDataSource[TD, EI, Q, A]],
    preparatorClass: Class[_ <: BasePreparator[TD, PD]],
    algorithmClassMap: Map[String, Class[_ <: BaseAlgorithm[PD, _, Q, P]]],
    servingClass: Class[_ <: BaseServing[Q, P]]) = this(
    Map("" -> dataSourceClass),
    Map("" -> preparatorClass),
    algorithmClassMap,
    Map("" -> servingClass)
  )


  def this(
    dataSourceClass: Class[_ <: BaseDataSource[TD, EI, Q, A]],
    preparatorClass: Class[_ <: BasePreparator[TD, PD]],
    algorithmClassMap: java.util.Map[String, Class[_ <: BaseAlgorithm[PD, _, Q, P]]],
    servingClass: Class[_ <: BaseServing[Q, P]]) = this(
    Map("" -> dataSourceClass),
    Map("" -> preparatorClass),
    JavaConversions.mapAsScalaMap(algorithmClassMap).toMap,
    Map("" -> servingClass)
  )

  def copy(
    dataSourceClassMap: Map[String, Class[_ <: BaseDataSource[TD, EI, Q, A]]]
      = dataSourceClassMap,
    preparatorClassMap: Map[String, Class[_ <: BasePreparator[TD, PD]]]
    = preparatorClassMap,
    algorithmClassMap: Map[String, Class[_ <: BaseAlgorithm[PD, _, Q, P]]]
    = algorithmClassMap,
    servingClassMap: Map[String, Class[_ <: BaseServing[Q, P]]]
    = servingClassMap): Engine[TD, EI, PD, Q, P, A] = {
    new Engine(
      dataSourceClassMap,
      preparatorClassMap,
      algorithmClassMap,
      servingClassMap)
  }

  def train(
    sc: SparkContext,
    engineParams: EngineParams,
    engineInstanceId: String,
    params: WorkflowParams): Seq[Any] = {

    val (dataSourceName, dataSourceParams) = engineParams.dataSourceParams
    val dataSource = Doer(dataSourceClassMap(dataSourceName), dataSourceParams)

    val (preparatorName, preparatorParams) = engineParams.preparatorParams
    val preparator = Doer(preparatorClassMap(preparatorName), preparatorParams)

    val algoParamsList = engineParams.algorithmParamsList
    require(
      algoParamsList.size > 0,
      "EngineParams.algorithmParamsList must have at least 1 element."
    )

    val algorithms = algoParamsList.map { case (algoName, algoParams) =>
      Doer(algorithmClassMap(algoName), algoParams)
    }

    val models = Engine.train(
      sc, dataSource, preparator, algorithms, params)

    val algoCount = algorithms.size
    val algoTuples: Seq[(String, Params, BaseAlgorithm[_, _, _, _], Any)] =
      (0 until algoCount).map { ax => {
        val (name, params) = algoParamsList(ax)
        (name, params, algorithms(ax), models(ax))
      }}

    makeSerializableModels(
      sc,
      engineInstanceId = engineInstanceId,
      algoTuples = algoTuples
    )
  }

  private[MBA]
  def prepareDeploy(
    sc: SparkContext,
    engineParams: EngineParams,
    engineInstanceId: String,
    persistentModels: Seq[Any],
    params: WorkflowParams): Seq[Any] = {

    val algoParamsList = engineParams.algorithmParamsList
    val algorithms = algoParamsList.map { case (algoName, algoParams) =>
      Doer(algorithmClassMap(algoName), algoParams)
    }

    val models = if (persistentModels.exists(m => m.isInstanceOf[Unit.type])) {
      logger.info("Some persisted models are Unit, need to re-train.")
      val (dataSourceName, dataSourceParams) = engineParams.dataSourceParams
      val dataSource = Doer(dataSourceClassMap(dataSourceName), dataSourceParams)

      val (preparatorName, preparatorParams) = engineParams.preparatorParams
      val preparator = Doer(preparatorClassMap(preparatorName), preparatorParams)

      val td = dataSource.readTrainingBase(sc)
      val pd = preparator.prepareBase(sc, td)

      val models = algorithms.zip(persistentModels).map { case (algo, m) =>
        m match {
          case Unit => algo.trainBase(sc, pd)
          case _ => m
        }
      }
      models
    } else {
      logger.info("Using persisted model")
      persistentModels
    }

    models
      .zip(algorithms)
      .zip(algoParamsList)
      .zipWithIndex
      .map {
        case (((model, algo), (algoName, algoParams)), ax) => {
          model match {
            case modelManifest: PersistentModelManifest => {
              logger.info("Custom-persisted model detected for algorithm " +
                algo.getClass.getName)
              SparkWorkflowUtils.getPersistentModel(
                modelManifest,
                Seq(engineInstanceId, ax, algoName).mkString("-"),
                algoParams,
                Some(sc),
                getClass.getClassLoader)
            }
            case m => {
              try {
                logger.info(
                  s"Loaded model ${m.getClass.getName} for algorithm " +
                    s"${algo.getClass.getName}")
                m
              } catch {
                case e: NullPointerException =>
                    logger.warn(
                      s"Loaded model ${m.getClass.getName} for algorithm " +
                        s"${algo.getClass.getName}")
                    m
              }
            }
          }
        }
    }
  }

  private def makeSerializableModels(
    sc: SparkContext,
    engineInstanceId: String,
    algoTuples: Seq[(String, Params, BaseAlgorithm[_, _, _, _], Any)]
    ): Seq[Any] = {

    logger.info(s"engineInstanceId=$engineInstanceId")

    algoTuples
      .zipWithIndex
      .map { case ((name, params, algo, model), ax) =>
        algo.makePersistentModel(
          sc = sc,
          modelId = Seq(engineInstanceId, ax, name).mkString("-"),
          algoParams = params,
          bm = model)
    }
  }

  def eval(
    sc: SparkContext,
    engineParams: EngineParams,
    params: WorkflowParams): Seq[(EI, RDD[(Q, P, A)])] = {

    val (dataSourceName, dataSourceParams) = engineParams.dataSourceParams
    val dataSource = Doer(dataSourceClassMap(dataSourceName), dataSourceParams)

    val (preparatorName, preparatorParams) = engineParams.preparatorParams
    val preparator = Doer(preparatorClassMap(preparatorName), preparatorParams)

    val algoParamsList = engineParams.algorithmParamsList
    require(
      algoParamsList.size > 0,
      "EngineParams.algorithmParamsList must have at least 1 element.")

    val algorithms = algoParamsList.map { case (algoName, algoParams) => {
      try {
        Doer(algorithmClassMap(algoName), algoParams)
      } catch {
        case e: NoSuchElementException => {
          if (algoName == "") {
            logger.error("Empty algorithm name supplied but it could not " +
              "match with any algorithm in the engine's definition. " +
              "Existing algorithm name(s) are: " +
              s"${algorithmClassMap.keys.mkString(", ")}. Aborting.")
          } else {
            logger.error(s"${algoName} cannot be found in the engine's " +
              "definition. Existing algorithm name(s) are: " +
              s"${algorithmClassMap.keys.mkString(", ")}. Aborting.")
          }
          sys.exit(1)
        }
      }
    }}

    val (servingName, servingParams) = engineParams.servingParams
    val serving = Doer(servingClassMap(servingName), servingParams)

    Engine.eval(sc, dataSource, preparator, algorithms, serving)
  }

  override def jValueToEngineParams(
    variantJson: JValue,
    jsonExtractor: JsonExtractorOption): EngineParams = {

    val engineLanguage = EngineLanguage.Scala

    logger.info(s"Extracting datasource params...")
    val dataSourceParams: (String, Params) =
      WorkflowUtils.getParamsFromJsonByFieldAndClass(
        variantJson,
        "datasource",
        dataSourceClassMap,
        engineLanguage,
        jsonExtractor)
    logger.info(s"Datasource params: $dataSourceParams")

    logger.info(s"Extracting preparator params...")
    val preparatorParams: (String, Params) =
      WorkflowUtils.getParamsFromJsonByFieldAndClass(
        variantJson,
        "preparator",
        preparatorClassMap,
        engineLanguage,
        jsonExtractor)
    logger.info(s"Preparator params: $preparatorParams")

    val algorithmsParams: Seq[(String, Params)] =
      variantJson findField {
        case JField("algorithms", _) => true
        case _ => false
      } map { jv =>
        val algorithmsParamsJson = jv._2
        algorithmsParamsJson match {
          case JArray(s) => s.map { algorithmParamsJValue =>
            val eap = algorithmParamsJValue.extract[CreateWorkflow.AlgorithmParams]
            (
              eap.name,
              WorkflowUtils.extractParams(
                engineLanguage,
                compact(render(eap.params)),
                algorithmClassMap(eap.name),
                jsonExtractor)
              )
          }
          case _ => Nil
        }
      } getOrElse Seq(("", EmptyParams()))

    logger.info(s"Extracting serving params...")
    val servingParams: (String, Params) =
      WorkflowUtils.getParamsFromJsonByFieldAndClass(
        variantJson,
        "serving",
        servingClassMap,
        engineLanguage,
        jsonExtractor)
    logger.info(s"Serving params: $servingParams")

    new EngineParams(
      dataSourceParams = dataSourceParams,
      preparatorParams = preparatorParams,
      algorithmParamsList = algorithmsParams,
      servingParams = servingParams
    )

  }
}

object Engine {
  private type EX = Int
  private type AX = Int
  private type QX = Long

  @transient lazy private val logger = Logger[this.type]

  class DataSourceMap[TD, EI, Q, A](
    val m: Map[String, Class[_ <: BaseDataSource[TD, EI, Q, A]]]) {
    def this(c: Class[_ <: BaseDataSource[TD, EI, Q, A]]) = this(Map("" -> c))
  }


  object DataSourceMap {
    implicit def cToMap[TD, EI, Q, A](
      c: Class[_ <: BaseDataSource[TD, EI, Q, A]]):
    DataSourceMap[TD, EI, Q, A] = new DataSourceMap(c)
    implicit def mToMap[TD, EI, Q, A](
      m: Map[String, Class[_ <: BaseDataSource[TD, EI, Q, A]]]):
    DataSourceMap[TD, EI, Q, A] = new DataSourceMap(m)
  }

  class PreparatorMap[TD, PD](
    val m: Map[String, Class[_ <: BasePreparator[TD, PD]]]) {
    def this(c: Class[_ <: BasePreparator[TD, PD]]) = this(Map("" -> c))
  }

  object PreparatorMap {
    implicit def cToMap[TD, PD](
      c: Class[_ <: BasePreparator[TD, PD]]):
    PreparatorMap[TD, PD] = new PreparatorMap(c)
    implicit def mToMap[TD, PD](
      m: Map[String, Class[_ <: BasePreparator[TD, PD]]]):
    PreparatorMap[TD, PD] = new PreparatorMap(m)
  }

  class ServingMap[Q, P](
    val m: Map[String, Class[_ <: BaseServing[Q, P]]]) {
    def this(c: Class[_ <: BaseServing[Q, P]]) = this(Map("" -> c))
  }

  object ServingMap {
    implicit def cToMap[Q, P](
      c: Class[_ <: BaseServing[Q, P]]): ServingMap[Q, P] =
      new ServingMap(c)
    implicit def mToMap[Q, P](
      m: Map[String, Class[_ <: BaseServing[Q, P]]]): ServingMap[Q, P] =
      new ServingMap(m)
  }

  def apply[TD, EI, PD, Q, P, A](
    dataSourceMap: DataSourceMap[TD, EI, Q, A],
    preparatorMap: PreparatorMap[TD, PD],
    algorithmClassMap: Map[String, Class[_ <: BaseAlgorithm[PD, _, Q, P]]],
    servingMap: ServingMap[Q, P]): Engine[TD, EI, PD, Q, P, A] =
      new Engine(
        dataSourceMap.m,
        preparatorMap.m,
        algorithmClassMap,
        servingMap.m)

  def train[TD, PD, Q](
    sc: SparkContext,
    dataSource: BaseDataSource[TD, _, Q, _],
    preparator: BasePreparator[TD, PD],
    algorithmList: Seq[BaseAlgorithm[PD, _, Q, _]],
    params: WorkflowParams): Seq[Any] = {

    logger.info("EngineWorkflow.train")
    logger.info(s"DataSource: $dataSource")
    logger.info(s"Preparator: $preparator")
    logger.info(s"AlgorithmList: $algorithmList")

    if (params.skipSanityCheck) {
      logger.info("Data sanity check is off.")
    } else {
      logger.info("Data santiy check is on.")
    }

    val td = try {
      dataSource.readTrainingBase(sc)
    } catch {
      case e: StorageClientException =>
        logger.error(s"Error occured reading from data source. (Reason: " +
          e.getMessage + ") Please see the log for debugging details.", e)
        sys.exit(1)
    }

    if (params.stopAfterRead) {
      logger.info("Stopping here because --stop-after-read is set.")
      throw StopAfterReadInterruption()
    }

    val pd = preparator.prepareBase(sc, td)

    if (!params.skipSanityCheck) {
      pd match {
        case sanityCheckable: SanityCheck => {
          logger.info(s"${pd.getClass.getName} supports data sanity" +
            " check. Performing check.")
          sanityCheckable.sanityCheck()
        }
        case _ => {
          logger.info(s"${pd.getClass.getName} does not support" +
            " data sanity check. Skipping check.")
        }
      }
    }

    if (params.stopAfterPrepare) {
      logger.info("Stopping here because --stop-after-prepare is set.")
      throw StopAfterPrepareInterruption()
    }

    val models: Seq[Any] = algorithmList.map(_.trainBase(sc, pd))

    if (!params.skipSanityCheck) {
      models.foreach { model => {
        model match {
          case sanityCheckable: SanityCheck => {
            logger.info(s"${model.getClass.getName} supports data sanity" +
              " check. Performing check.")
            sanityCheckable.sanityCheck()
          }
          case _ => {
            logger.info(s"${model.getClass.getName} does not support" +
              " data sanity check. Skipping check.")
          }
        }
      }}
    }

    logger.info("EngineWorkflow.train completed")
    models
  }

  def eval[TD, PD, Q, P, A, EI](
    sc: SparkContext,
    dataSource: BaseDataSource[TD, EI, Q, A],
    preparator: BasePreparator[TD, PD],
    algorithmList: Seq[BaseAlgorithm[PD, _, Q, P]],
    serving: BaseServing[Q, P]): Seq[(EI, RDD[(Q, P, A)])] = {

    logger.info(s"DataSource: $dataSource")
    logger.info(s"Preparator: $preparator")
    logger.info(s"AlgorithmList: $algorithmList")
    logger.info(s"Serving: $serving")

    val algoMap: Map[AX, BaseAlgorithm[PD, _, Q, P]] = algorithmList
      .zipWithIndex
      .map(_.swap)
      .toMap
    val algoCount = algoMap.size

    val evalTupleMap: Map[EX, (TD, EI, RDD[(Q, A)])] = dataSource
      .readEvalBase(sc)
      .zipWithIndex
      .map(_.swap)
      .toMap

    val evalCount = evalTupleMap.size

    val evalTrainMap: Map[EX, TD] = evalTupleMap.mapValues(_._1)
    val evalInfoMap: Map[EX, EI] = evalTupleMap.mapValues(_._2)
    val evalQAsMap: Map[EX, RDD[(QX, (Q, A))]] = evalTupleMap
      .mapValues(_._3)
      .mapValues{ _.zipWithUniqueId().map(_.swap) }

    val preparedMap: Map[EX, PD] = evalTrainMap.mapValues { td => {
      preparator.prepareBase(sc, td)
    }}

    val algoModelsMap: Map[EX, Map[AX, Any]] = preparedMap.mapValues { pd => {
      algoMap.mapValues(_.trainBase(sc, pd))
    }}

    val algoPredictsMap: Map[EX, RDD[(QX, Seq[P])]] = (0 until evalCount)
      .map { ex => {
        val modelMap: Map[AX, Any] = algoModelsMap(ex)

        val qs: RDD[(QX, Q)] = evalQAsMap(ex).mapValues(_._1)

        val algoPredicts: Seq[RDD[(QX, (AX, P))]] = (0 until algoCount)
          .map { ax => {
            val algo = algoMap(ax)
            val model = modelMap(ax)
            val rawPredicts: RDD[(QX, P)] = algo.batchPredictBase(sc, model, qs)
            val predicts: RDD[(QX, (AX, P))] = rawPredicts.map { case (qx, p) => {
              (qx, (ax, p))
            }}
            predicts
        }}

      val unionAlgoPredicts: RDD[(QX, Seq[P])] = sc.union(algoPredicts)
        .groupByKey
        .mapValues { ps => {
          assert(ps.size == algoCount, "Must have same length as algoCount")
          ps.toSeq.sortBy(_._1).map(_._2)
      }}

      (ex, unionAlgoPredicts)


    }}.toMap

    val servingQPAMap: Map[EX, RDD[(Q, P, A)]] = algoPredictsMap
      .map { case (ex, psMap) => {
        val qasMap: RDD[(QX, (Q, A))] = evalQAsMap(ex)
        val qpsaMap: RDD[(QX, Q, Seq[P], A)] = psMap.join(qasMap)
          .map { case (qx, t) => (qx, t._2._1, t._1, t._2._2)}

        val qpaMap: RDD[(Q, P, A)] = qpsaMap.map {
          case (qx, q, ps, a) => (q, serving.serveBase(q, ps), a)
        }
        (ex, qpaMap)
    }}

    (0 until evalCount).map { ex => {
      (evalInfoMap(ex), servingQPAMap(ex))
    }}
    .toSeq

  }

}
