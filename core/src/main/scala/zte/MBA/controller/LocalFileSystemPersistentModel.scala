package zte.MBA.controller

import org.apache.spark.SparkContext


trait LocalFileSystemPersistentModel[AP <: Params] extends PersistentModel[AP] {
  def save(id: String, params: AP, sc: SparkContext): Boolean = {
    Utils.save(id, this)
    true
  }
}

trait LocalFileSystemPersistentModelLoader[AP <: Params, M]
  extends PersistentModelLoader[AP, M] {
  def apply(id: String, params: AP, sc: Option[SparkContext]): M = {
    Utils.load(id).asInstanceOf[M]
  }
}


