package zte.MBA.core


abstract class BaseServing[Q, P]
  extends AbstractDoer {
  private [MBA]
  def serveBase(q: Q, ps: Seq[P]): P
}
