package zte.MBA.controller

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import zte.MBA.core.BasePreparator

import scala.reflect.ClassTag


abstract class LPreparator[TD, PD: ClassTag]
  extends BasePreparator[RDD[TD], RDD[PD]] {

  private [MBA]
  def prepareBase(sc: SparkContext, rddTD: RDD[TD]): RDD[PD] = {
    rddTD.map(prepare)
  }

  def prepare(trainingData: TD): PD
}
