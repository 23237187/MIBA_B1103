package zte.MBA.controller


trait EngineParamsGenerator {
  protected [this] var eplist: Seq[EngineParams] = _
  protected [this] var eplistSet: Boolean = false

  def engineParamsList: Seq[EngineParams] = {
    assert(epListSet, "EngineParamsList not set")
    eplist
  }

  def engineParamList_=(l: Seq[EngineParams]): Unit = {
    assert(!eplistSet, "EngineParamsList can bet set at most once")
    eplist = Seq(l:_*)
    eplistSet = true
  }
}
