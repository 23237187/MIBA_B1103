package zte.MBA.controller

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import zte.MBA.core.BaseAlgorithm

import scala.reflect.ClassTag


abstract class LAlgorithm[PD, M: ClassTag, Q, P]
  extends BaseAlgorithm[RDD[PD], RDD[M], Q, P] {

  private[MBA]
  def trainBase(sc: SparkContext, pd: RDD[PD]): RDD[M] = pd.map(train)

  def train(pd: PD): M

  private[MBA]
  def batchPredictBase(sc: SparkContext, bm: Any, qs: RDD[(Long, Q)]): RDD[(Long, P)] = {
    val mRDD = bm.asInstanceOf[RDD[M]]
    batchPredict(mRDD, qs)
  }

  private[MBA]
  def batchPredict(mRDD: RDD[M], qs: RDD[(Long, Q)]): RDD[(Long, P)] = {
    val glomQs: RDD[Array[(Long, Q)]] = qs.glom()
    val cartesian: RDD[(M, Array[(Long, Q)])] = mRDD.cartesian(glomQs)
    cartesian.flatMap {case (m, qArray) =>
      qArray.map { case (qx, q) => (qx, predict(m, q))}
    }
  }

  private[MBA]
  def predictBase(localBaseModel: Any, q: Q): P = {
    predict(localBaseModel.asInstanceOf[M], q)
  }

  def predict(m: M, q: Q): P

  private[MBA]
  override
  def makePersistentModel(
    sc: SparkContext,
    modelId: String,
    algoParams: Params,
    bm: Any): Any = {

    val m = bm.asInstanceOf[RDD[M]].first()
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
