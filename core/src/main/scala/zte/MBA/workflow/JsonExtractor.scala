package zte.MBA.workflow

import com.google.gson.{GsonBuilder, Gson, TypeAdapterFactory}
import org.json4s.reflect.TypeInfo
import org.json4s.{Extraction, Formats}
import zte.MBA.controller.Utils
import zte.MBA.workflow.JsonExtractorOption.JsonExtractorOption

import org.json4s.native.JsonMethods._


object JsonExtractor {

  

  def extract[T](
    extractorOption: JsonExtractorOption,
    json: String,
    clazz: Class[T],
    json4sFormats: Formats = Utils.json4sDefaultFormats,
    gsonTypeAdapterFactories: Seq[TypeAdapterFactory] = Seq.empty[TypeAdapterFactory]): T = {

    extractorOption match {
      case JsonExtractorOption.Both =>
        try {
          extractWithJson4sNative(json, json4sFormats, clazz)
        } catch {
          case e: Exception =>
            extractWithGson(json, clazz, gsonTypeAdapterFactories)
        }
      case JsonExtractorOption.Json4SNative =>
        extractWithJson4sNative(json, json4sFormats, clazz)
      case JsonExtractorOption.Gson =>
        extractWithGson(json, clazz, gsonTypeAdapterFactories)
    }
  }

  private def extractWithJson4sNative[T](
    json: String,
    formats: Formats,
    clazz: Class[T]): T = {

    Extraction.extract(parse(json), TypeInfo(clazz, None))(formats).asInstanceOf[T]
  }

  private def extractWithGson[T](
    json: String,
    clazz: Class[T],
    gsonTypeAdapterFactories: Seq[TypeAdapterFactory]): T = {

    gson(gsonTypeAdapterFactories).fromJson(json, clazz)
  }

  private def gson(gsonTypeAdapterFactories: Seq[TypeAdapterFactory]): Gson = {
    val gsonBuilder = new GsonBuilder()
    gsonTypeAdapterFactories.foreach { typeAdapterFactory =>
      gsonBuilder.registerTypeAdapterFactory(typeAdapterFactory)
    }

    gsonBuilder.create()
  }
}
