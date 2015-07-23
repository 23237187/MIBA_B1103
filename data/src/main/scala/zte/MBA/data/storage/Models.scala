package zte.MBA.data.storage

import com.google.common.io.BaseEncoding
import org.json4s.{DefaultFormats, CustomSerializer}
import org.json4s.JsonAST.{JString, JField, JObject}


case class Model(
                id: String,
                models: Array[Byte]
                  )

trait Models {
  def insert(i: Model): Unit

  def get(id: String): Option[Model]

  def delete(id: String): Unit
}

class ModelSerializer extends CustomSerializer[Model](
  format => ({
    case JObject(fields) =>
      implicit val formats = DefaultFormats
      val seed = Model(
        id = "",
        models = Array[Byte]()
      )
      fields.foldLeft(seed) { case (i, field) =>
        field match {
          case JField("id", JString(id)) => i.copy(id = id)
          case JField("models", JString(models)) =>
            i.copy(models = BaseEncoding.base64.decode(models))
          case _ => i
        }
      }
  },
  {
    case i: Model =>
      JObject(
        JField("id", JString(i.id)) ::
        JField("models", JString(BaseEncoding.base64.encode(i.models))) ::
        Nil
      )
  }
    )
)

