package zte.MBA.controller


trait Params extends Serializable {

}

case class EmptyParams() extends Params {
  override def toString(): String = "Empty"
}
