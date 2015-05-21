package zte.MBA.data.storage

import grizzled.slf4j.Logging

import scala.reflect.api.TypeTags.TypeTag
import scala.reflect.api.TypeTags.TypeTag

import scala.reflect.runtime.universe._


trait BaseStorageClient {
  val config: StorageClientConfig
  val client: AnyRef
  val prefix: String = ""
}

case class StorageClientConfig(
  parallel: Boolean = false,
  test: Boolean = false,
  properties: Map[String, String] = Map())

class StorageClientException(message: String, cause: Throwable)
  extends RuntimeException(message, cause)

class StorageException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message:String) = this(message, null)
}

//当需要一个与Event存储的接口时，使用这个对象
object Storage extends Logging{
  private case class ClientMeta(
    sourceType: String,
    client: BaseStorageClient,
    config: StorageClientConfig)

  private case class DataObjectMeta(sourceName: String, namespace: String)

  private var errors = 0

  private val sourcesPrefix = "MBA_STORAGE_SOURCES"

  private val sourceTypesRegex = """MBA_STORAGE_SOURCES_([^_]+)_TYPE""".r

  private val sourceKeys: Seq[String] = sys.env.keys.toSeq.flatMap {
    k =>
      sourceTypesRegex findFirstIn k match {
        case Some(sourceTypesRegex(sourceType)) => Seq(sourceType)
        case None => Nil
    }
  }

  if (sourceKeys.size == 0) warn("There is no properly configured data source.")

  private val s2cm = scala.collection.mutable.Map[String, Option[ClientMeta]]()

  private val EventDataRepository = "EVENTDATA"
  private val ModelDataRepository = "MODELDATA"
  private val MetaDataRepository = "METADATA"

  private val repositoriesPrefix = "MBA_STORAGE_REPOSITORIES"

  private val repositoryNamesRegex = """MBA_STORAGE_REPOSITORIES_([^_]+)_NAME""".r

  private val repositoryKeys: Seq[String] = sys.env.keys.toSeq.flatMap {
    k =>
      repositoryNamesRegex findFirstIn k match {
        case Some(repositoryNamesRegex(repositoryName)) => Seq(repositoryName)
        case None => Nil
      }
  }

  if (repositoryKeys.size == 0) warn("There is no properly configured repository.")

  private val requiredRepositories = Seq(MetaDataRepository)

  requiredRepositories foreach {
    r =>
      if (!repositoryKeys.contains(r)) {
        error(s"Required repository (${r}) configuration is missing.")
        errors += 1
      }
  }

  //获取repositories对应的数据对象元信息的map
  private val repositoriesToDataObjectMeta: Map[String, DataObjectMeta] =
    repositoryKeys.map(r => //r就是repo的标识
        try {
          val keyedPath = repositoriesPrefixPath(r)
          val name = sys.env(prefixPath(keyedPath, "NAME"))
          val sourceName = sys.env(prefixPath(keyedPath, "SOURCE"))
          if (sourceKeys.contains(sourceName)) {
            r -> DataObjectMeta(
              sourceName = sourceName,
              namespace = name
            )
          } else {
            error(s"$sourceName is not a configured storage source.")
            r -> DataObjectMeta("", "")
          }
        } catch {
          case e: Throwable =>
            error(e.getMessage)
            errors += 1
            r -> DataObjectMeta("", "")
        }
    ).toMap

  if (errors > 0) {
    error(s"There were $errors configuration errors. Exiting.")
    sys.exit(errors)
  }

  //构造器和字段定义结束，下面开始定义方法

  private def prefixPath(prefix: String, body: String) = s"${prefix}_$body"

  private def sourcePrefixPath(body: String) = prefixPath(sourcesPrefix, body)

  private def repositoriesPrefixPath(body: String) = prefixPath(repositoriesPrefix, body)

  //将源(数据库类型字符串)转化为对应的连接元信息
  private def sourcesToClientMeta(
      source: String,
      parallel: Boolean,
      test: Boolean): Option[ClientMeta] = {
    val sourceName = if (parallel) s"parallel-$source" else source
    s2cm.getOrElseUpdate(sourceName, updateS2CM(source, parallel, test))
  }

  //利用对应数据库的访问实现(类名), 建立对数据库的连接
  private def getClient(
      clientConfig: StorageClientConfig,
      pkg: String): BaseStorageClient = {
    val className = "zte.MBA.data.storage." + pkg + ".StorageClient"
    try {
      Class.forName(className).getConstructors()(0).newInstance(clientConfig).
        asInstanceOf[BaseStorageClient]
    } catch {
      case e: ClassNotFoundException =>
        val originalClassName = pkg + ".StorageClient"
        Class.forName(originalClassName).getConstructors()(0).
          newInstance(clientConfig).asInstanceOf[BaseStorageClient]
      case e: java.lang.reflect.InvocationTargetException =>
        throw e.getCause
    }
  }

  private def updateS2CM(k: String, parallel: Boolean, test: Boolean):
    Option[ClientMeta] = {
    try {
      val keyedPath = sourcePrefixPath(k)
      val sourceType = sys.env(prefixPath(keyedPath, "TYPE"))
      val props = sys.env.filter(t => t._1.startsWith(keyedPath)).map(
        t => t._1.replace(s"${keyedPath}_", "") -> t._2)
      val clientConfig = StorageClientConfig(
        properties = props,
        parallel = parallel,
        test = test)
      val client = getClient(clientConfig, sourceType)//建立了连接
      Some(ClientMeta(sourceType, client, clientConfig))
    } catch {
      case e: Throwable =>
        error(s"Error initializing storage client for source ${k}", e)
        errors += 1
        None
    }
  }

  private[MBA]
  def getDataObject[T](repo: String, test: Boolean = false)
    (implicit tag: TypeTag[T]): T = {
    val repoDOMeta = repositoriesToDataObjectMeta(repo)
    val repoDOSourceName = repoDOMeta.sourceName
    getDataObject[T](repoDOSourceName, repoDOMeta.namespace, test = test)
  }

  private[MBA]
  def getPDataObject[T](repo: String)(implicit tag: TypeTag[T]): T = {
    val repoDOMeta = repositoriesToDataObjectMeta(repo)
    val repoDOSourceName = repoDOMeta.sourceName
    getPDataObject[T](repoDOSourceName, repoDOMeta.namespace)
  }

  private[MBA] def getDataObject[T](
      sourceName: String, //数据源名
      namespace: String, //子表名
      parallel: Boolean = false,
      test: Boolean = false)(implicit tag: TypeTag[T]): T = {
    //获取连接的元信息
    val clientMeta = sourcesToClientMeta(sourceName, parallel, test) getOrElse {
      throw new StorageClientException(
        s"Data Source $sourceName was not properly initialized.", null)
    }
    val sourceType = clientMeta.sourceType
    val ctorArgs = dataObjectCtorArgs(clientMeta.client, namespace)
    //去找具体类,以便能够建立连接
    val classPrefix = clientMeta.client.prefix
    val originalClassName = tag.tpe.toString.split('.')
    val rawClassName = sourceType + "." + classPrefix + originalClassName.last
    val className = "zte.MBA.data.storage." + rawClassName
    val clazz = try {
      Class.forName(className)
    } catch {
      case e: ClassNotFoundException =>
        try {
          Class.forName(rawClassName)
        } catch {
          case e: ClassNotFoundException =>
            throw new StorageClientException("No storage backend " +
              "implementation can be found (tried both " +
              s"$className and $rawClassName)", e)
        }
    }
    //建立到数据访问的连接
    val constructor = clazz.getConstructors()(0)
    try {
      constructor.newInstance(ctorArgs: _*).asInstanceOf[T]
    } catch {
      case e: IllegalArgumentException =>
        error(
          "Unable to instantiate data object with class '" +
            constructor.getDeclaringClass.getName + " because its constructor" +
            " does not have the right number of arguments." +
            " Number of required constructor arguments: " +
            ctorArgs.size + "." +
            " Number of existing constructor arguments: " +
            constructor.getParameterTypes.size + "." +
            s" Storage source name: ${sourceName}." +
            s" Exception message: ${e.getMessage}).", e)
        errors += 1
        throw e
      case e: java.lang.reflect.InvocationTargetException =>
        throw e.getCause
    }
  }

  private def getPDataObject[T](
      sourceName: String,
      databaseName: String)(implicit tag: TypeTag[T]): T = {
    getDataObject[T](sourceName, databaseName, true)
  }

  private def dataObjectCtorArgs(
      client: BaseStorageClient,
      namespace: String): Seq[AnyRef] = {
    Seq(client.client, client.config, namespace)
  }

  private[MBA] def verifyAllDataObjects(): Unit = {
    info("Verifying Meta Data Backend (Source: " +
      s"${repositoriesToDataObjectMeta(MetaDataRepository).sourceName})...")
    getMetaDataEngineManifests()
    getMetaDataEngineInstances()
    getMetaDataEvaluationInstances()
    getMetaDataApps()
    getMetaDataAccessKeys()
    info("Verifying Model Data Backend (Source: " +
      s"${repositoriesToDataObjectMeta(ModelDataRepository).sourceName})...")
    getModelDataModels()
    info("Verifying Event Data Backend (Source: " +
      s"${repositoriesToDataObjectMeta(EventDataRepository).sourceName})...")
    val eventsDb = getLEvents(test = true)
    info("Test writing to Event Store (App Id 0)...")
    //使用 appid=0 来测试
    eventsDb.init(0)
    eventsDb.insert(Event(
      event = "test",
      entityType = "test",
      entityId = "test"), 0)
    eventsDb.remove(0)
    eventsDb.close()
  }

  private[MBA] def getMetaDataEngineManifests(): EngineManifests =
    getDataObject[EngineManifests](MetaDataRepository)

  private[MBA] def getMetaDataEngineInstances(): EngineInstances =
    getDataObject[EngineInstances](MetaDataRepository)

  private[MBA] def getMetaDataEvaluationInstances(): EvaluationInstances =
    getDataObject[EvaluationInstances](MetaDataRepository)

  private[MBA] def getMetaDataApps(): Apps =
    getDataObject[Apps](MetaDataRepository)

  private[MBA] def getMetaDataAccessKeys(): AccessKeys =
    getDataObject[AccessKeys](MetaDataRepository)

  private[MBA] def getMetaDataChannels(): Channels =
    getDataObject[Channels](MetaDataRepository)

  private[MBA] def getModelDataModels(): Models =
    getDataObject[Models](ModelDataRepository)

  //获得一个本地数据存储中的[[Event]]数据访问对象
  def getLEvents(test: Boolean = false): LEvents =
    getDataObject[LEvents](EventDataRepository, test = test)

  //获得一个RDD的[[Event]]数据访问对象
  def getPEvents(): PEvents =
    getDataObject[PEvents](EventDataRepository)

  def config: Map[String, Map[String, Map[String, String]]] = Map(
    "sources" -> s2cm.toMap.map {
      case (source, clientMeta) =>
        source -> clientMeta.map {
          cm =>
            Map(
              "type" -> cm.sourceType,
              "config" -> cm.config.properties.map(t => s"${t._1} -> ${t._2}").mkString(", ")
            )
        }.getOrElse(Map.empty)
    }
  )
}
