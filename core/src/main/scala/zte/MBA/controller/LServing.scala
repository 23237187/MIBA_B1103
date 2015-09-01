package zte.MBA.controller

import zte.MBA.core.BaseServing


abstract class LServing[Q, P] extends BaseServing[Q, P] {
  private [MBA]
  def serveBase(q: Q, ps: Seq[P]): P = {
    serve(q, ps)
  }

  def serve(query: Q, predictions: Seq[P]): P
}
