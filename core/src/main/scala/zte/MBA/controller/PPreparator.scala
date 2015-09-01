package zte.MBA.controller

import org.apache.spark.SparkContext
import zte.MBA.core.BasePreparator


abstract class PPreparator[TD, PD]
  extends BasePreparator[TD, PD] {

  private [MBA]
  def prepareBase(sc: SparkContext, td: TD): PD = {
    prepare(sc, td)
  }

  def prepare(sc: SparkContext, trainingData: TD): PD
}
