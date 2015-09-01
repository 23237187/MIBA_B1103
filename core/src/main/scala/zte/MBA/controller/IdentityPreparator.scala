package zte.MBA.controller

import org.apache.spark.SparkContext
import zte.MBA.core.{BaseDataSource, BasePreparator}


class IdentityPreparator[TD] extends BasePreparator[TD, TD] {
  private [MBA]
  def prepareBase(sc: SparkContext, td: TD): TD = td
}

object IdentityPreparator {
  def apply[TD](ds: Class[_ <: BaseDataSource[TD, _, _, _]]): Class[IdentityPreparator[TD]] =
    classOf[IdentityPreparator[TD]]
}


