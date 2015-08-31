package zte.MBA.controller

import zte.MBA.core.BaseEngine


trait Deployment extends EngineFactory {
  protected[this] var _engine: BaseEngine[_,_,_,_] = _
  protected[this] var engineSet: Boolean = false

  def apply(): BaseEngine[_,_,_,_] = {
    assert(engineSet, "Engine not set")
    _engine
  }

  private[MBA]
  def engine: BaseEngine[_,_,_,_] = {
    assert(engineSet, "Engine not set")
    _engine
  }

  def engine_=[EI, Q, P, A](engine: BaseEngine[EI, Q, P, A]) {
    assert(!engineSet, "Engine can be set at most once")
    _engine = engine
    engineSet = true
  }

}
