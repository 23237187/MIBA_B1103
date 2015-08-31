package zte.MBA.controller

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import zte.MBA.core.BaseAlgorithm
import zte.MBA.workflow.PersistentModelManifest

import scala.reflect.ClassTag


abstract class P2LAlgotithm[PD, M: ClassTag, Q:ClassTag, P]
  extends BaseAlgorithm[PD, M, Q, P] {

  private[MBA]
  def trainBase(sc: SparkContext, pd: PD): M = train(sc, pd)

  def train(sc: SparkContext, pd: PD): M

  private[MBA]
  def batchPredictBase(sc: SparkContext, bm: Any, qs: RDD[(Long, Q)])
  : RDD[(Long, P)] = batchPredict(bm.asInstanceOf[M], qs)

  private[MBA]
  def batchPredict(m: M, qs: RDD[(Long, Q)]): RDD[(Long, P)] = {
    qs.mapValues { q => predict(m, q)}
  }

  private[MBA]
  def predictBase(bm: Any, q: Q): P = {
    predict(bm.asInstanceOf[M], q)
  }

  def predict(model: M, query: Q): P

  private[MBA]
  override
  def makePersistentModel(
    sc: SparkContext,
    modelId: String,
    algoParams: Params,
    bm: Any): Any = {
    val m = bm.asInstanceOf[M]
    if (m.isInstanceOf[PersistentModel[_]]) {
      if (m.asInstanceOf[PersistentModel[Params]].save(
        modelId, algoParams, sc)) {
        PersistentModelManifest(className = m.getClass.getName)
      } else {
        Unit
      }
    } else {
      m
    }
  }
}
