package zte.MBA.data.storage

case class Channel(
  id: Int,
  name: String,
  appid: Int){

  require(Channel.isValidName(name),
    "Invalid channel name: ${name}. ${Channel.nameConstraint}")
}

object Channel {
  def isValidName(s: String): Boolean = {
    s.matches("^[a-zA-Z0-9-]{1,16}$")
  }

  val nameConstraint: String =
    "Only alphanumeric and - characters are allowed and max length is 16."
}

trait Channels {

  def insert(channel: Channel): Option[Int]

  def get(id: Int): Option[Channel]

  def getByAppid(appid: Int): Seq[Channel]

  def delete(id: Int): Boolean

}
