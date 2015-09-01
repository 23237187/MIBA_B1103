package zte.MBA.controller

import zte.MBA.core.BaseAlgorithm


class LFirstServing[Q, P] extends LServing[Q, P] {
  def serve(query: Q, predictions: Seq[P]): P = predictions.head
}

object LFirstServing {
  def apply[Q, P](a: Class[_ <: BaseAlgorithm[_, _, Q, P]]): Class[LFirstServing[Q, P]] =
    classOf[LFirstServing[Q, P]]
}
