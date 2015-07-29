package zte.MBA.controller

import zte.MBA.core.{BaseAlgorithm, BaseDataSource}


class EngineParams(
  val dataSourceParams: (String, Params) = ("", EmptyParams()),
  val preparatorParams: (String, Params) = ("", EmptyParams()),
  val algorithmParamsList: Seq[(String, Params)] = Seq(),
  val servingParams: (String, Params) = ("", EmptyParams())
    extends Serializable {

  def copy(
    dataSourceParams: (String, Params) = dataSourceParams,
    preparatorParams: (String, Params) = preparatorParams,
    algorithmParamsList: Seq[(String, Params)] = algorithmParamsList,
    servingParams: (String, Params) = servingParams): EngineParams = {

    new EngineParams(
      dataSourceParams,
      preparatorParams,
      algorithmParamsList,
      servingParams)
  }
}

object EngineParams {

  def apply(
    dataSourceName: String = "",
    dataSourceParams: Params = EmptyParams(),
    preparatorName: String = "",
    preparatorParams: Params = EmptyParams(),
    algorithmParamsList: Seq[(String, Params)] = Seq(),
    servingName: String = "",
    servingParams: Params = EmptyParams()): EngineParams = {
      new EngineParams(
        dataSourceParams = (dataSourceName, dataSourceParams),
        preparatorParams = (preparatorName, preparatorParams),
        algorithmParamsList = algorithmParamsList,
        servingParams = (servingName, servingParams)
      )
  }
}

class SimpleEngine[TD, EI, Q, P, A](
  dataSourceClass: Class[_ <: BaseDataSource[TD, EI, Q, A]],
  algorithmClass: Class[_ <: BaseAlgorithm[TD, _, Q, P]])
  extends Engine(
    dataSourceClass,
    IndentityPreparator(dataSourceClass),
    Map("" -> algorithmClass),
    LFirstServing(algorithmClass))

class SimpleEngineParams(
  dataSourceParams: Params = EmptyParams(),
  algorithmParams: Params = EmptyParams())
  extends EngineParams(
            dataSourceParams = ("", dataSourceParams),
            algorithmParamsList = Seq(("", algorithmParams)))


