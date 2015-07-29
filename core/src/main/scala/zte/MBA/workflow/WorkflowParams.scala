package zte.MBA.workflow


case class WorkflowParams(
  batch: String = "",
  verbose: Int = 2,
  saveModel: Boolean = true,
  sparkEnv: Map[String, String] =
    Map[String, String]("spark.executor.extraClassPath" -> "."),
  skipSanityCheck: Boolean = false,
  stopAfterRead: Boolean = false,
  stopAfterPrepare: Boolean = false) {

  def this(batch: String, verbose: Int, saveModel: Boolean) =
    this(batch, verbose, saveModel, Map[String, String]())
}

