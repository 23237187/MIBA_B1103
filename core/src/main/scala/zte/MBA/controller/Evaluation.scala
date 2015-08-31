package zte.MBA.controller

import zte.MBA.core.{BaseEngine, BaseEvaluatorResult, BaseEvaluator}


trait Evaluation extends Deployment {
  protected [this] var _evaluatorSet: Boolean = false
  protected [this] var _evaluator: BaseEvaluator[_, _, _, _, _ <: BaseEvaluatorResult] = _

  private [MBA]
  def evaluator: BaseEvaluator[_, _, _, _, _ <: BaseEvaluatorResult] = {
    assert(_evaluatorSet, "Evaluator not set")
    _evaluator
  }

  def engineEvaluator: (BaseEngine[_,_,_,_], BaseEvaluator[_, _, _, _, _ <: BaseEvaluatorResult]) = {
    assert(_evaluatorSet, "Evaluator not set")
    (engine, _evaluator)
  }

  def engineEvaluator_=[EI, Q, P, A, R <: BaseEvaluatorResult](
    engineEvaluator: (
      BaseEngine[EI, Q, P, A],
        BaseEvaluator[EI, Q, P, A, R])) {
    assert(!_evaluatorSet, "Evaluator can be set at most once")
    engine = engineEvaluator._1
    _evaluator = engineEvaluator._2
    _evaluatorSet = true
  }

  def engineMetric: (BaseEngine[_,_,_,_], Metric[_,_,_,_,_]) = {
    throw new NotImplementedError("This method is to keep the compiler happy")
  }

  def engineMetric_=[EI, Q, P, A](
    engineMetric: (BaseEngine[EI, Q, P, A], Metric[EI, Q, P, A, _])) {
    engineEvaluator = (
      engineMetric._1,
      MetricEvaluator(
        metric = engineMetric._2,
        otherMetrics = Seq[Metric[EI, Q, P, A, _]](),
        outputPath = "best.json"))
  }

  private [prediction]
  def engineMetrics: (BaseEngine[_, _, _, _], Metric[_, _, _, _, _]) = {
    throw new NotImplementedError("This method is to keep the compiler happy")
  }

  def engineMetrics_=[EI, Q, P, A](
    engineMetrics: (
      BaseEngine[EI, Q, P, A],
        Metric[EI, Q, P, A, _],
        Seq[Metric[EI, Q, P, A, _]])) {
    engineEvaluator = (
      engineMetrics._1,
      MetricEvaluator(engineMetrics._2, engineMetrics._3))
  }
}
