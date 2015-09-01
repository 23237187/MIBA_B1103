package zte.MBA.controller

import zte.MBA.core.BaseAlgorithm


class LAverageServing[Q] extends LServing[Q, Double] {
  def serve(query: Q, predictions: Seq[Double]): Double = {
    predictions.sum / predictions.length
  }
}

object LAverageServing {
  def apply[Q](a: Class[_ <: BaseAlgorithm[_, _, Q, _]]): Class[LAverageServing[Q]] =
    classOf[LAverageServing[Q]]
}
