package zte.MBA.controller

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import zte.MBA.core.BaseDataSource

import scala.reflect.ClassTag


abstract class LDataSouce[TD: ClassTag, EI: ClassTag, Q, A]
  extends BaseDataSource[RDD[TD], EI, Q, A] {

  private [MBA]
  def readTrainingBase(sc: SparkContext): RDD[TD] = {
    sc.parallelize(Seq(None)).map(_ => readTraining())
  }

  def readTraining(): TD

  private [MBA]
  def readEvalBase(sc: SparkContext): Seq[(RDD[TD], EI, RDD[(Q, A)])] = {
    val localEvalData: Seq[(TD, EI, Seq[(Q, A)])] = readEval()

    localEvalData.map { case (td, ei, qaSeq) => {
      val tdRDD = sc.parallelize(Seq(None)).map(_ => td)
      val qaRDD = sc.parallelize(qaSeq)
      (tdRDD, ei, qaRDD)
    }}
  }

  def readEval(): Seq[(TD, EI, Seq[(Q, A)])] = Seq[(TD, EI, Seq[(Q, A)])]()

}
