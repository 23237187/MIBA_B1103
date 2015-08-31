package zte.MBA.workflow

import com.google.common.io.ByteStreams
import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.joda.time.DateTime
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.native.JsonMethods.parse
import zte.MBA.controller.Engine
import zte.MBA.core.BaseEngine
import zte.MBA.data.storage.{EvaluationInstance, EngineInstance, Storage}
import zte.MBA.workflow.JsonExtractorOption.JsonExtractorOption


object CreateWorkflow extends Logging {

  case class WorkflowConfig(
    deployMode: String = "",
    batch: String = "",
    engineId: String = "",
    engineVersion: String = "",
    engineVariant: String = "",
    engineFactory: String = "",
    engineParamsKey: String = "",
    evaluationClass: Option[String] = None,
    engineParamsGeneratorClass: Option[String] = None,
    env: Option[String] = None,
    skipSanityCheck: Boolean = false,
    stopAfterRead: Boolean = false,
    stopAfterPrepare: Boolean = false,
    verbosity: Int = 0,
    verbose: Boolean = false,
    debug: Boolean = false,
    logFile: Option[String] = None,
    jsonExtractor: JsonExtractorOption = JsonExtractorOption.Both)

  case class AlgorithmParams(name: String, params: JValue)

  val hadoopConf = new Configuration
  val hdfs = FileSystem.get(hadoopConf)
  val localfs = FileSystem.getLocal(hadoopConf)

  private def stringFromFile(
    basePath: String,
    filePath: String,
    fs: FileSystem = hdfs): String = {

    try {
      val p =
        if (basePath == "") {
          new Path(filePath)
        } else {
          new Path(basePath + Path.SEPARATOR + filePath)
        }
      new String(ByteStreams.toByteArray(fs.open(p)).map(_.toChar))
    } catch {
      case e: java.io.IOException =>
        error(s"Error reading from file: ${e.getMessage}. Aborting workflow.")
        sys.exit(1)
    }
  }

  val parser = new scopt.OptionParser[WorkflowConfig]("CreateWorkflow") {
    opt[String]("batch") action { (x, c) =>
      c.copy(batch = x)
    } text("Batch label of the workflow run.")
    opt[String]("engine-id") required() action { (x, c) =>
      c.copy(engineId = x)
    } text("Engine's ID.")
    opt[String]("engine-version") required() action { (x, c) =>
      c.copy(engineVersion = x)
    } text("Engine's version.")
    opt[String]("engine-variant") required() action { (x, c) =>
      c.copy(engineVariant = x)
    } text("Engine variant JSON.")
    opt[String]("evaluation-class") action { (x, c) =>
      c.copy(evaluationClass = Some(x))
    } text("Class name of the run's evaluator.")
    opt[String]("engine-params-generator-class") action { (x, c) =>
      c.copy(engineParamsGeneratorClass = Some(x))
    } text("Path to evaluator parameters")
    opt[String]("env") action { (x, c) =>
      c.copy(env = Some(x))
    } text("Comma-separated list of environmental variables (in 'FOO=BAR' " +
      "format) to pass to the Spark execution environment.")
    opt[Unit]("verbose") action { (x, c) =>
      c.copy(verbose = true)
    } text("Enable verbose output.")
    opt[Unit]("debug") action { (x, c) =>
      c.copy(debug = true)
    } text("Enable debug output.")
    opt[Unit]("skip-sanity-check") action { (x, c) =>
      c.copy(skipSanityCheck = true)
    }
    opt[Unit]("stop-after-read") action { (x, c) =>
      c.copy(stopAfterRead = true)
    }
    opt[Unit]("stop-after-prepare") action { (x, c) =>
      c.copy(stopAfterPrepare = true)
    }
    opt[String]("deploy-mode") action { (x, c) =>
      c.copy(deployMode = x)
    }
    opt[Int]("verbosity") action { (x, c) =>
      c.copy(verbosity = x)
    }
    opt[String]("engine-factory") action { (x, c) =>
      c.copy(engineFactory = x)
    }
    opt[String]("engine-params-key") action { (x, c) =>
      c.copy(engineParamsKey = x)
    }
    opt[String]("log-file") action { (x, c) =>
      c.copy(logFile = Some(x))
    }
    opt[String]("json-extractor") action { (x, c) =>
      c.copy(jsonExtractor = JsonExtractorOption.withName(x))
    }
  }

  def main(args: Array[String]): Unit = {
    val wfcOpt = parser.parse(args, WorkflowConfig())
    if (wfcOpt.isEmpty) {
      logger.error("WorkflowConfig is empty. Quitting")
      return
    }

    val wfc = wfcOpt.get

    WorkflowUtils.modifyLogging(wfc.verbose)
    val targetfs = if (wfc.deployMode == "cluster") hdfs else localfs
    val variantJson = parse(stringFromFile("", wfc.engineVariant, targetfs))
    val engineFactory = if (wfc.engineFactory == "") {
      variantJson \ "engineFactory" match {
        case JString(s) => s
        case _ =>
          error("Unable to read engine factory class name from " +
            s"${wfc.engineVariant}. Aborting.")
          sys.exit(1)
      }
    } else wfc.engineFactory
    val variantId = variantJson \ "id" match {
      case JString(s) => s
      case _ =>
        error("Unable to read engine variant ID from " +
          s"${wfc.engineVariant}. Aborting.")
        sys.exit(1)
    }
    val (engineLanguage, engineFactoryObj) = try {
      WorkflowUtils.getEngine(engineFactory, getClass.getClassLoader)
    } catch {
      case e @ (_: ClassNotFoundException | _: NoSuchMethodException) =>
        error(s"Unable to obtain engine: ${e.getMessage}. Aborting workflow.")
        sys.exit(1)
    }

    val engine: BaseEngine[_, _, _, _] = engineFactoryObj()

    val evaluation = wfc.evaluationClass.map { ec =>
      try {
        WorkflowUtils.getEvaluation(ec, getClass.getClassLoader)._2
      } catch {
        case e @ (_: ClassNotFoundException | _: NoSuchMethodException) =>
          error(s"Unable to obtain evaluation $ec. Aborting workflow.", e)
          sys.exit(1)
      }
    }

    val engineParamsGenerator = wfc.engineParamsGeneratorClass.map { epg =>
      try {
        WorkflowUtils.getEngineParamsGenerator(epg, getClass.getClassLoader)._2
      } catch {
        case e @ (_: ClassNotFoundException | _: NoSuchMethodException) =>
          error(s"Unable to obtain engine parameters generator $epg. " +
            "Aborting workflow.", e)
          sys.exit(1)
      }
    }

    val mbaEnvVars = wfc.env.map( e =>
      e.split(',').flatMap( p =>
        p.split('=') match {
          case Array(k, v) => List(k -> v)
          case _ => Nil
        }
      ).toMap
    ).getOrElse(Map())

    val customSparkConf = WorkflowUtils.extractSparkConf(variantJson)
    val workflowParams = WorkflowParams(
      verbose = wfc.verbosity,
      skipSanityCheck = wfc.skipSanityCheck,
      stopAfterPrepare = wfc.stopAfterPrepare,
      stopAfterRead = wfc.stopAfterRead,
      sparkEnv = WorkflowParams().sparkEnv ++ customSparkConf
    )

    if (evaluation.isEmpty) {
      if (!engine.isInstanceOf[Engine[_,_,_,_,_,_]]) {
        throw new NoSuchMethodException(s"Engine $engine is not trainable")
      }

      val trainableEngine = engine.asInstanceOf[Engine[_,_,_,_,_,_]]

      val engineParams = if (wfc.engineParamsKey == "") {
        trainableEngine.jValueToEngineParams(variantJson, wfc.jsonExtractor)
      } else {
        engineFactoryObj.engineParams(wfc.engineParamsKey)
      }

      val engineInstance = EngineInstance(
        id = "",
        status = "INIT",
        startTime = DateTime.now(),
        endTime = DateTime.now(),
        engineId = wfc.engineId,
        engineVersion = wfc.engineVersion,
        engineVariant = variantId,
        engineFactory = engineFactory,
        batch = if (wfc.batch == "") engineFactory else wfc.batch,
        env = mbaEnvVars,
        sparkConf = workflowParams.sparkEnv,
        dataSourceParams =
          JsonExtractor.paramToJson(wfc.jsonExtractor, engineParams.dataSourceParams),
        preparatorParams =
          JsonExtractor.paramToJson(wfc.jsonExtractor, engineParams.preparatorParams),
        algorithmsParams =
          JsonExtractor.paramsToJson(wfc.jsonExtractor, engineParams.algorithmParamsList),
        servingParams =
          JsonExtractor.paramToJson(wfc.jsonExtractor, engineParams.servingParams)
      )

      val engineInstanceId = Storage.getMetaDataEngineInstances.insert(engineInstance)

      CoreWorkflow.runTrain(
        env = mbaEnvVars,
        params = workflowParams,
        engine = trainableEngine,
        engineParams = engineParams,
        engineInstance = engineInstance.copy(id = engineInstanceId)
      )

    } else {
      val evaluationInstance = EvaluationInstance(
        evaluationClass = wfc.evaluationClass.get,
        engineParamsGeneratorClass = wfc.engineParamsGeneratorClass.get,
        batch = wfc.batch,
        env = mbaEnvVars,
        sparkConf = workflowParams.sparkEnv
      )
      Workflow.runEvaluation(
        evaluation = evaluation.get,
        engineParamsGenerator = engineParamsGenerator.get,
        evaluationInstance = evaluationInstance,
        params = workflowParams)
    }
  }



}
