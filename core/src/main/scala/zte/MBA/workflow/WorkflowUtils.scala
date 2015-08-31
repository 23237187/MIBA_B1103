package zte.MBA.workflow

import java.io.FileNotFoundException

import com.google.gson.{JsonSyntaxException, Gson}
import grizzled.slf4j.Logging
import org.apache.log4j.{LogManager, Level}
import org.apache.spark.SparkContext
import org.json4s.{BuildInfo, MappingException, Formats, CustomSerializer}
import org.json4s.JsonAST._
import zte.MBA.controller.{EngineFactory, EmptyParams, Params, Utils}
import org.json4s.native.JsonMethods._
import zte.MBA.workflow.JsonExtractorOption.JsonExtractorOption

import scala.reflect.runtime.universe
import scala.io.Source

object WorkflowUtils extends Logging {
  @transient private lazy val gson = new Gson

  def extractNameParams(jv: JValue): NameParams = {
    implicit val formats = Utils.json4sDefaultFormats
    val nameOpt = (jv \ "name").extract[Option[String]]
    val paramsOpt = (jv \ "params").extract[Option[JValue]]

    if (nameOpt.isEmpty && paramsOpt.isEmpty) {
      error("Unable to find 'name' or 'params' fields in" +
        s" ${compact(render(jv))}.\n" +
        "The 'params' field is required in engine.json" +
        " in order to specify parameters for DataSource, Preparator or" +
        " Serving.\n")
      sys.exit(1)
    }

    if (nameOpt.isEmpty) {
      info(s"No 'name' is found. Default empty String will be used.")
    }

    if (paramsOpt.isEmpty) {
      info(s"No 'params' is found. Default EmptyParams will be used.")
    }

    NameParams(
      name =  nameOpt.getOrElse(""),
      params = paramsOpt
    )
  }

  def getParamsFromJsonByFieldAndClass(
    variantJaon: JValue,
    field: String,
    classMap: Map[String, Class[_]],
    engineLanguage: EngineLanguage.Value,
    jsonExtractor: JsonExtractorOption): (String, Params) = {

    variantJaon findField {
      case JField(f, _) => f == field
      case _ => false
    } map { jv =>
      implicit lazy val formats = Utils.json4sDefaultFormats + new NameParamsSerializer
      val np: NameParams = try {
        jv._2.extract[NameParams]
      } catch {
        case e: Exception =>
          error(s"Unable to extract $field name and params $jv")
          throw e
      }

      val extractedParams = np.params.map { p =>
        try {
          if (!classMap.contains(np.name)) {
            error(s"Unable to find $field class with name '${np.name}'" +
              " defined in Engine.")
            sys.exit(1)
          }
          WorkflowUtils.extractParams(
            engineLanguage,
            compact(render(p)),
            classMap(np.name),
            jsonExtractor,
            formats)
        } catch {
          case e: Exception =>
            error(s"Unable to extract $field params $p")
            throw e
        }
      }.getOrElse(EmptyParams())

      (np.name, extractedParams)
    } getOrElse("", EmptyParams())

  }

  def extractParams(
    language: EngineLanguage.Value = EngineLanguage.Scala,
    json: String,
    clazz: Class[_],
    jsonExtractor: JsonExtractorOption,
    formats: Formats = Utils.json4sDefaultFormats): Params = {

    implicit val f = formats
    val pClass = clazz.getConstructors.head.getParameterTypes
    if (pClass.size == 0) {
      if (json != "") {
        warn(s"Non-empty parameters supplied to ${clazz.getName}, but its " +
          "constructor does not accept any arguments. Stubbing with empty " +
          "parameters.")
      }
      EmptyParams()
    } else {
      val apClass = pClass.head
      try {
        JsonExtractor.extract(jsonExtractor, json, apClass, f).asInstanceOf[Params]
      } catch {
        case e@(_: MappingException | _: JsonSyntaxException) =>
          error(
            s"Unable to extract parameters for ${apClass.getName} from " +
              s"JSON string: $json. Aborting workflow.",
            e)
          throw e
      }
    }
  }

  def modifyLogging(verbose: Boolean): Unit = {
    val rootLoggerLevel = if (verbose) Level.TRACE else Level.INFO
    val chattyLoggerLevel = if (verbose) Level.INFO else Level.WARN

    LogManager.getRootLogger.setLevel(rootLoggerLevel)

    LogManager.getLogger("org.elasticsearch").setLevel(chattyLoggerLevel)
    LogManager.getLogger("org.apache.hadoop").setLevel(chattyLoggerLevel)
    LogManager.getLogger("org.apache.spark").setLevel(chattyLoggerLevel)
    LogManager.getLogger("org.eclipse.jetty").setLevel(chattyLoggerLevel)
    LogManager.getLogger("akka").setLevel(chattyLoggerLevel)
  }

  def getEngine(engine: String, cl: ClassLoader): (EngineLanguage.Value, EngineFactory) = {
    val runtimeMirror = universe.runtimeMirror(cl)
    val engineModule = runtimeMirror.staticModule(engine)
    val engineObject = runtimeMirror.reflectModule(engineModule)
    try {
      (
        EngineLanguage.Scala,
        engineObject.instance.asInstanceOf[EngineFactory]
        )
    } catch {
      case e @ (_: NoSuchFieldException | _: ClassNotFoundException) => try {
        (
          EngineLanguage.Java,
          Class.forName(engine).newInstance.asInstanceOf[EngineFactory]
          )
      }
    }
  }

  def getEvaluation(evaluation: String, cl: ClassLoader): (EngineLanguage.Value, Evaluation) = {
    val runtimeMirror = universe.runtimeMirror(cl)
    val evaluationModule = runtimeMirror.staticModule(evaluation)
    val evaluationObject = runtimeMirror.reflectModule(evaluationModule)
    try {
      (
        EngineLanguage.Scala,
        evaluationObject.instance.asInstanceOf[Evaluation]
        )
    } catch {
      case e @ (_: NoSuchFieldException | _: ClassNotFoundException) => try {
        (
          EngineLanguage.Java,
          Class.forName(evaluation).newInstance.asInstanceOf[Evaluation]
          )
      }
    }
  }

  def getEngineParamsGenerator(epg: String, cl: ClassLoader):
  (EngineLanguage.Value, EngineParamsGenerator) = {
    val runtimeMirror = universe.runtimeMirror(cl)
    val epgModule = runtimeMirror.staticModule(epg)
    val epgObject = runtimeMirror.reflectModule(epgModule)
    try {
      (
        EngineLanguage.Scala,
        epgObject.instance.asInstanceOf[EngineParamsGenerator]
        )
    } catch {
      case e @ (_: NoSuchFieldException | _: ClassNotFoundException) => try {
        (
          EngineLanguage.Java,
          Class.forName(epg).newInstance.asInstanceOf[EngineParamsGenerator]
          )
      }
    }
  }

  def extractSparkConf(root: JValue): List[(String, String)] = {
    def flatten(jv: JValue): List[(List[String], String)] = {
      jv match {
        case JObject(fields) =>
          for ((namePrefix, childJV) <- fields;
               (name, value) <- flatten(childJV))
            yield (namePrefix :: name) -> value
        case JArray(_) => {
          error("Arrays are not allowed in the sparkConf section of engine.js.")
          sys.exit(1)
        }
        case JNothing => List()
        case _ => List(List() -> jv.values.toString)
      }
    }

    flatten(root \ "sparkConf").map(x =>
      (x._1.reduce((a, b) => s"$a.$b"), x._2))
  }

  def mbaEnvVars: Map[String, String] =
    sys.env.filter(kv => kv._1.startsWith("MBA_"))

  private[mba] def checkUpgrade(
    component: String = "core",
    engine: String = ""): Unit = {
    val runner = new Thread(new UpgradeCheckRunner(component, engine))
    runner.start()
  }
}

case class NameParams(name: String, params: Option[JValue])

class NameParamsSerializer extends CustomSerializer[NameParams](format => ({
  case jv: JValue => WorkflowUtils.extractNameParams(jv)
}, {
  case x: NameParams =>
    JObject(JField("name", JString(x.name)) ::
      JField("params", x.params.getOrElse(JNothing)) :: Nil)
}))

object EngineLanguage extends Enumeration {
  val Scala, Java = Value
}

object SparkWorkflowUtils extends Logging {
  def getPersistentModel[AP <: Params, M](
    pmm: PersistentModelManifest,
    runId: String,
    params: AP,
    sc: Option[SparkContext],
    cl: ClassLoader): M = {

    val runtimeMirror = universe.runtimeMirror(cl)
    val pmmModule = runtimeMirror.staticModule(pmm.className)
    val pmmObject = runtimeMirror.reflectModule(pmmModule)
    try {
      pmmObject.instance.asInstanceOf[PersistentModelLoder[AP, M]](
        runId,
        params,
        sc)
    } catch {
      case e @ (_: NoSuchFieldException | _: ClassNotFoundException) => try {
        val loadMethod = Class.forName(pmm.className).getMethod(
          "load",
          classOf[String],
          classOf[Params],
          classOf[SparkContext])
        loadMethod.invoke(null, runId, params, sc.orNull).asInstanceOf[M]
      } catch {
        case e: ClassNotFoundException =>
          error(s"Model class ${pmm.className} cannot be found.")
          throw e
        case e: NoSuchMethodException =>
          error(
            "The load(String, Params, SparkContext) method cannot be found.")
          throw e
      }
    }
  }
}

class UpgradeCheckRunner(
  val component: String,
  val engine: String) extends Runnable with Logging {
  val version = BuildInfo.version
  val versionsHost = "http://direct.prediction.io/"

  def run(): Unit = {
    val url = if (engine == "") {
      s"$versionsHost$version/$component.json"
    } else {
      s"$versionsHost$version/$component/$engine.json"
    }
    try {
      val upgradeData = Source.fromURL(url)
    } catch {
      case e: FileNotFoundException => {
        debug(s"Update metainfo not found. $url")
      }
    }

  }
}

