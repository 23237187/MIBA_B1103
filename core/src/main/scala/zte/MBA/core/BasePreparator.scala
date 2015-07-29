package zte.MBA.core

import org.apache.spark.SparkContext


abstract class BasePreparator[TD, PD]
  extends AbstractDoer {
  private[MBA]
  def prepareBase(sc: SparkContext, td: TD): PD
}
