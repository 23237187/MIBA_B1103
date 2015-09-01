package zte.MBA.workflow


object JsonExtractorOption extends Enumeration {
  type JsonExtractorOption = Value
  val Json4sNative = Value
  val Gson = Value
  val Both = Value
}
