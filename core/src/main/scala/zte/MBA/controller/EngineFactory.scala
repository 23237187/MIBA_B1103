package zte.MBA.controller

import zte.MBA.core.BaseEngine


abstract class EngineFactory {

  def apply(): BaseEngine
}
