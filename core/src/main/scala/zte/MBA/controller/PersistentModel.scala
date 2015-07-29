package zte.MBA.controller

import org.apache.spark.SparkContext


trait PersistentModel[AP <: Params] {

  def save(id: String, params: AP, sc: SparkContext): Boolean
}

trait PersistentModelLoader[AP <: Params, M] {

  def apply(id: String, params: AP, sc: Option[SparkContext]): M
}


