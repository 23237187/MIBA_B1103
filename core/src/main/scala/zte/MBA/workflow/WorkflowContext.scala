package zte.MBA.workflow

import grizzled.slf4j.Logging
import org.apache.spark.{SparkConf, SparkContext}


object WorkflowContext extends Logging{
  def apply(
    batch: String = "",
    executorEnv: Map[String, String] = Map(),
    sparkEnv: Map[String, String] = Map(),
    mode: String = ""
    ): SparkContext = {

    val conf = new SparkConf()
    val prefix = if (mode == "") "MobileBehaviorAnalysis" else s"MobileBehaviorAnalysis ${mode}"
    conf.setAppName(s"${prefix}: ${batch}")

    debug(s"Executor environment received: ${executorEnv}")
    executorEnv.map(kv => conf.setExecutorEnv(kv._1, kv._2))
    debug(s"SparkConf executor environment: ${conf.getExecutorEnv}")
    debug(s"Application environment received: ${sparkEnv}")
    conf.setAll(sparkEnv)
    val sparkConfString = conf.getAll.toSeq
    debug(s"SparkConf environment: $sparkConfString")
    new SparkContext(conf)
  }
}

