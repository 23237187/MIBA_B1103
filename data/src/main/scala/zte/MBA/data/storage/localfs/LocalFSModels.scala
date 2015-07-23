package zte.MBA.data.storage.localfs

import java.io.{FileNotFoundException, FileOutputStream, File}

import grizzled.slf4j.Logging
import zte.MBA.data.storage.{Models, Model, StorageClientConfig}

import scala.io.Source


class LocalFSModels(f: File, config: StorageClientConfig, prefix: String)
  extends Models with Logging {
  def insert(i: Model): Unit = {
    try {
      val fos = new FileOutputStream(new File(f, s"${prefix}${i.id}"))
      fos.write(i.models)
      fos.close()
    } catch {
      case e: FileNotFoundException => error(e.getMessage)
    }
  }

  def get(id: String): Option[Model] = {
    try {
      Some(Model(
        id = id,
        models = Source.fromFile(new File(f, s"${prefix}${id}"))(
          scala.io.Codec.ISO8859).map(_.toByte).toArray))
    } catch {
      case e: Throwable =>
        error(e.getMessage)
        None
    }
  }

  def delete(id: String): Unit = {
    val m = new File(f, s"${prefix}${id}")
    if (!m.delete) error(s"Unable to delete ${m.getCanonicalPath}!")
  }
}
