package zte.MBA.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


abstract class BaseDataSource[TD, EI, Q, A] extends AbstractDoer {

  private[MBA]
  def readTrainingBase(sc: SparkContext): TD

  private[MBA]
  def readEvalBase(sc: SparkContext): Seq[(TD, EI, RDD[(Q, A)])]
}
