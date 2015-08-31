package zte.MBA.controller

import zte.MBA.core.BaseEngine


abstract class EngineFactory {

  def apply(): BaseEngine[_,_,_,_]

  def engineParams(key: String): EngineParams = EngineParams()
}
