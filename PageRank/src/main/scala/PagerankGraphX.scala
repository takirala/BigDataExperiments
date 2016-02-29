/*
* References - http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html#making-a-vertex-rdd
*
* */

import java.nio.file.attribute.FileAttribute
import java.nio.file.{FileAlreadyExistsException, StandardOpenOption, Files, Paths}

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.math.Ordering

object PagerankGraphX {

  def pageHash(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }

  def printTopPages(wikiData: String, masterUrl: String, topC: Int, iters: Int, path: String, sc: SparkContext): Graph[(Double, String), Double] = {
    Files.deleteIfExists(Paths.get(path))

    var t1 = System.currentTimeMillis()

    val wiki: RDD[String] = sc.textFile(wikiData).coalesce(20)

    //Define the article class
    case class Article(id: Long, title: String, xml: String)

    var r: String = ""
    val articles = wiki.map(_.split('\t')).
      filter(line => line.length > 1).
      map(line => new Article(line(0).trim.toLong, line(1).trim, line(3).trim)).cache()

    write("Total articles ===> " + articles.count(), path)

    val vertices: RDD[(VertexId, String)] = articles.map(a => (pageHash(a.title), a.title))

    write("Total vertices ===> " + vertices.count(), path)

    val pattern = "<target>.+?<\\/target>".r

    val edges: RDD[Edge[Double]] = articles.flatMap { a =>
      val srcId = pageHash(a.title)
      pattern.findAllIn(a.xml).map { link =>
        val dstId = pageHash(link.replace("<target>", "").replace("</target>", ""))
        Edge(srcId, dstId, 1.0)
      }
    }
    write("Total edges ===> " + edges.count(), path)

    val graph = Graph(vertices, edges, "").subgraph(vpred = { (v, d) => d.nonEmpty }).cache

    write("Graph built ===> Vertices " + graph.vertices.count() + "\t Edges " + graph.edges.count() + " Triplets " + graph.triplets.count, path)
    write("Time taken to build graph : " + (System.currentTimeMillis() - t1), path)

    t1 = System.currentTimeMillis()

    val prGraph: Graph[Double, Double] = graph.staticPageRank(iters).cache

    val titleAndPrGraph: Graph[(Double, String), Double] = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    var p = titleAndPrGraph.vertices.top(titleAndPrGraph.triplets.count().toInt) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.take(topC).foreach(t => {
      write(t._2._2 + " has rank: " + t._2._1, path)
    })
    write("Total time taken => " + (System.currentTimeMillis() - t1 ), path)
    titleAndPrGraph
  }

  def write(r: String, path: String) = {
    print(r + "\n")
    Files.write(Paths.get(path), (r + "\n").getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
}