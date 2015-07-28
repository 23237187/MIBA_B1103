package zte.MBA.controller

import org.json4s.DefaultFormats
import org.json4s.ext.JodaTimeSerializers


object Utils {
  val json4sDefaultFormats = DefaultFormats.lossless ++ JodaTimeSerializers.all


}
