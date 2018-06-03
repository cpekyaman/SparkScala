import java.nio.charset.CodingErrorAction

import commons.Constants

import scala.io.{Codec, Source}

/**
  * Created by raistlin on 8/13/2017.
  */
package object section6 {
  def loadMovieNames() : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    Source.fromFile(s"${Constants.resourcesRootPath}/data/ml/u.item")
      .getLines()
      .map(_.split("\\|"))
      .filter(_.length > 1)
      .map(a => (a(0).toInt, a(1)))
      .toMap
  }
}
