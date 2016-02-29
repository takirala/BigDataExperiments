/*
* References - http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html#making-a-vertex-rdd
* Created by takirala on 2/27/2016.
* */

import java.nio.file.{StandardOpenOption, Paths, Files}
import java.util.Scanner

import org.jsoup.Jsoup
import scala.io.Source

import scala.math.Ordering
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {

  var wikiData = "C:\\dataset\\freebase-wex-2009-01-12-articles.tsv"
  var masterUrl = "local"
  val topC = 100
  val iters = 5

  val graphxresult = "graphx-result.log"
  val sparkresult = "spark-result.log"
  val top100result = "top100-result.log"

  def main(args: Array[String]) {

    //Arg1 - wiki data location.
    if (args.length >= 1) {
      wikiData = args(0).toString.trim
    }
    println("Dataset directory : " + wikiData)

    //Arg2 - master location.
    if (args.length >= 2) {
      masterUrl = args(1).toString.trim
    }
    println("Master URL : " + masterUrl)

    println("Top count : " + topC)

    val sparkConf = new SparkConf().setAppName("PageRank-43394921")
    val sc = new SparkContext(sparkConf)

    var scan = new Scanner(System.in)
    var loop = true
    var list: Graph[(Double, String), Double] = null

    while (loop) {
      Thread.sleep(500)
      println("===============================================================================")
      println("Enter option to run: \n\t1 - Pagerank on GraphX(Default) \n\t2 - Pagerank on Spark \n\t3 - Top 100 List")
      println("Waiting..")
      var l = scan.nextLine()
      var opt = 1
      if (l.length > 0) {
        opt = l.toInt
      }
      println("Read " + opt)

      if (opt == 1) {
        list = PagerankGraphX.printTopPages(wikiData, masterUrl, topC, iters, graphxresult, sc)
      }

      if (opt == 2) {
        PagerankSpark.printTopPages(wikiData, masterUrl, topC, iters, sparkresult, sc)
      }

      if (opt == 3) {
        if (list == null) {
          println("Evaluation pagerank using GraphX!!")
          list = PagerankGraphX.printTopPages(wikiData, masterUrl, topC, iters, graphxresult, sc)
        }
        Files.deleteIfExists(Paths.get(top100result))

        write("\n==============================================================", top100result)
        val l = Source.fromURL(getClass.getResource("/ScrapedList.txt")).getLines().toList

        var p = list.vertices.top(list.triplets.count().toInt) {
          Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
        }.filter{ x => 
          l contains (x._2._2)
        }.take(100).foreach(x => write("\n" + x._2._2 + " has rank : " + x._2._1, top100result))
      }

      if (opt == -1) {
        System.exit(0)
        sc.stop()
      }
    }
  }

  def write(r: String, path: String) = {
    println(r)
    Files.write(Paths.get(path), r.getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }

  def scrapeList(): List[String] = {
    var univs = List[String]()
    var URLS = for (i <- 2 to 27) yield s"http://www.4icu.org/reviews/index$i.htm"
    for (url <- URLS) {
      Thread.sleep(100)
      println("Scraping " + url)
      var doc = Jsoup.connect(url).maxBodySize(204800000).timeout(100000).get()
      // get all links
      var links = doc.select("img[src$=.png][width=16]");
      var it = links.iterator()
      while (it.hasNext) {
        var link = it.next()
        var u = link.attr("alt")
        univs = univs ::: (List(u.trim))
      }
    }
    univs
  }
}
