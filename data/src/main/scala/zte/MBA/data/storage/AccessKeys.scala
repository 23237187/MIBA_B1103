package zte.MBA.data.storage


case class AccessKey(
  key: String,
  appid: Int,
  events: Seq[String])

trait AccessKeys {
  def insert(k: AccessKey): Option[String]

  def get(k: String): Option[AccessKey]

  def getAll(): Seq[AccessKey]

  def getByAppid(appid: Int): Seq[AccessKey]

  def update(k: AccessKey): Boolean

  def delete(k: String): Boolean
}

