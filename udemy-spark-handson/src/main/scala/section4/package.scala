import java.nio.charset.CodingErrorAction

import scala.io.{Codec, Source}

/**
  * Created by raistlin on 7/31/2017.
  */
package object section4 {
  sealed trait NodeColor
  case object White extends NodeColor
  case object Grey extends NodeColor
  case object Black extends NodeColor

  def minColor(color1: NodeColor, color2: NodeColor): NodeColor = {
    var color: NodeColor = White

    if (color1 == White && (color2 == Grey || color2 == Black)) {
      color = color2
    }
    if (color1 == Grey && color2 == Black) {
      color = color2
    }
    if (color2 == White && (color1 == Grey || color1 == Black)) {
      color = color1
    }
    if (color2 == Grey && color1 == Black) {
      color = color1
    }

    color
  }

  val maxDistance = 9999
  val initColor = White

  case class BFSData(connections: Seq[Int], distance: Int, color: NodeColor) {
    def nextLevel(): BFSData = {
      BFSData(List.empty[Int], distance + 1, Grey)
    }

    def mark(): BFSData = {
      if(color == Grey) {
        BFSData(connections, distance, Black)
      } else {
        this
      }
    }
  }
  type BFSNode = (Int, BFSData)
  def BFSNode(id: Int, data: BFSData): BFSNode = { (id, data) }

  def loadMovieNames() : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    Source.fromFile("./UdemySparkCourse/src/main/resources/data/ml/u.item")
      .getLines()
      .map(_.split("\\|"))
      .filter(_.length > 1)
      .map(a => (a(0).toInt, a(1)))
      .toMap
  }
}
