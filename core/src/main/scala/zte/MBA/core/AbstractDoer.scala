package zte.MBA.core

import grizzled.slf4j.Logging
import zte.MBA.controller.Params


abstract class AbstractDoer extends Serializable

object Doer extends Logging {
  def apply[C <: AbstractDoer] (
    cls: Class[_ <: C], params: Params): C = {
    try {
      val constr = cls.getConstructor(params.getClass)
      constr.newInstance(params)
    } catch {
      case e: NoSuchMethodException => try {
        val zeroConstr = cls.getConstructor()
        zeroConstr.newInstance()
      } catch {
        case e: NoSuchElementException =>
          error(s"${params.getClass.getName} was used as the constructor " +
            s"argument to ${e.getMessage}, but no constructor can handle it. " +
            "Aborting.")
          sys.exit(1)
      }
    }
  }
}
