package zte.MBA.data.storage

case class App(
  id: Int,
  name: String,
  description: Option[String]
  )

trait Apps {
  def insert(app: App): Option[Int]

  def get(id: Int): Option[App]

  def getByName(name: String): Option[App]

  def getAll(): Seq[App]

  def update(app: App): Boolean

  def delete(id: Int): Boolean
}
