package section4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ListBuffer

/**
  * Created by raistlin on 7/30/2017.
  */
object SparkBFS {
  def mapper(target: Int, acc: LongAccumulator)(node: BFSNode): Seq[BFSNode] = {
    val newNodes = ListBuffer.empty[BFSNode]

    if(node._2.color == Grey) {
      node._2.connections.foreach { conn =>
        newNodes += BFSNode(conn, node._2.nextLevel())

        if(conn == target) {
          acc.add(1)
        }
      }
    }

    newNodes += BFSNode(node._1, node._2.mark())
    newNodes.toList
  }

  def reducer(first: BFSData, second: BFSData): BFSData = {
    BFSData(first.connections ++ second.connections, Math.min(first.distance, second.distance), minColor(first.color, second.color))
  }

  def createBFSNode(startHero: Int)(line: String): BFSNode = {
    val arr = line.split("\\s+").toList
    val currentHero = arr.head.toInt

    if(currentHero == startHero) {
      (currentHero, BFSData(arr.tail.map(_.toInt), 0, Grey))
    } else {
      (currentHero, BFSData(arr.tail.map(_.toInt), maxDistance, White))
    }
  }

  def createStartRDD(sc: SparkContext, startHero: Int): RDD[BFSNode] = {
    val filePath = "./UdemySparkCourse/src/main/resources/data/marvel/Marvel-graph.txt"
    sc.textFile(filePath).map(createBFSNode(startHero))
  }

  def solve(bfsData: RDD[BFSNode], targetHero: Int, acc: LongAccumulator): Unit = {
    var dataSet = bfsData
    for(it <- 1 to 10 if acc.isZero){
      println(s"Running iteration $it")

      val mappedNodes = dataSet.flatMap(mapper(targetHero, acc))
      println(s"${mappedNodes.count()} nodes are returned")

      dataSet = mappedNodes.reduceByKey(reducer)
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val startHero = 5306
    val targetHero = 14

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "SparkBFS")

    val bfsData = createStartRDD(sc, startHero)
    val acc: LongAccumulator = sc.longAccumulator("HitCounter")

    solve(bfsData, targetHero, acc)
  }
}
