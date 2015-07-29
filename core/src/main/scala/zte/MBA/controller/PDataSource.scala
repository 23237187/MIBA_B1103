package zte.MBA.controller

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import zte.MBA.core.BaseDataSource


abstract class PDataSource[TD, EI, Q, A]
  extends BaseDataSource[TD, EI, Q, A] {

  private[MBA]
  def readTrainingBase(sc: SparkContext): TD = readTraining(sc)

  def readTraining(sc: SparkContext): TD

  private[MBA]
  def readEvalBase(sc:SparkContext): Seq[(TD, EI, RDD[(Q, A)])] = readEval(sc)

  def readEval(sc: SparkContext): Seq[(TD, EI, RDD[(Q, A)])] =
    Seq[(TD, EI, RDD[(Q, A)])]()


}
