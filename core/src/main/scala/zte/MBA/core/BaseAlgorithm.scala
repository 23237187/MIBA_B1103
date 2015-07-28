package zte.MBA.core

import com.google.gson.TypeAdapterFactory
import net.jodah.typetools.TypeResolver
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import zte.MBA.controller.{Params, Utils}


trait BaseQuerySerializer {
  @transient lazy val querySerializer = Utils.json4sDefaultFormats
  @transient lazy val gsonTypeAdapterFactories = Seq.empty[TypeAdapterFactory]
}

abstract class BaseAlgorithm[PD, M, Q, P]
  extends AbstractDoer with BaseQuerySerializer {
  private[MBA]
  def trainBase(sc: SparkContext, pd: PD): M

  private[MBA]
  def batchPredictBase(sc: SparkContext, bm: Any, qs: RDD[(Long, Q)]): RDD[(Long, P)]

  private[MBA]
  def makePersistentModel(
    sc: SparkContext,
    modelId: String,
    algoParams: Params,
    bm: Any): Any = Unit

  private[MBA]
  def predictBase(bm: Any, q: Q): P

  private[MBA] def queryClass = {
    val types = TypeResolver.resolveRawArguments(classOf[BaseAlgorithm[PD, M, Q, P]], getClass)
    types(2).asInstanceOf[Class[Q]]
  }
}
