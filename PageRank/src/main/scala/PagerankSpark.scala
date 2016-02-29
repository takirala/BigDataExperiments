
import java.nio.file.{StandardOpenOption, Paths, Files}

import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by takirala on 2/21/2016.
  */
object PagerankSpark {

  def printTopPages(wikiData: String, masterUrl: String, topC: Int, iters: Int, path: String, sc: SparkContext) {

    var t1 = System.currentTimeMillis()
    val wiki: RDD[String] = sc.textFile(wikiData, 1)
    Files.deleteIfExists(Paths.get(path))

    //Define the article and link between articles classes.
    case class Article(val id: Long, val title: String, val xml: String)

    val articles = wiki.map(_.split('\t')).
      filter(line => line.length > 1).
      // Add a link to itself as an entry. (Optional)
      //"<target>" + line(1).trim + "</target>"
      map(line => new Article(line(0).trim.toLong, line(1).trim, line(3).trim)).cache()

    write("Total articles ===> " + articles.count(), path)

    val pattern = "<target>.+?<\\/target>".r

    val links: RDD[(String, Iterable[String])] = articles.flatMap { a =>

      pattern.findAllIn(a.xml).map { link =>
        val dstId = link.replace("<target>", "").replace("</target>", "")
        (a.title, dstId)
      }
    }.distinct().groupByKey().cache()

    write("Total Links Iters ====================================> " + links.count(), path)

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {

      println("Iteration ====================================> " + i)
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      println("Contribs ====================================> " + contribs.count())
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      println("Ranks ====================================> " + ranks.count())
    }


    var output1 = ranks.sortBy(_._2, false).collect()

    output1.take(topC).foreach(tup => {
      write(tup._1 + " has rank: " + tup._2, path)
    })
  }

  def write(r: String, path: String) = {
    print(r + "\n")
    Files.write(Paths.get(path), (r + "\n").getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
}