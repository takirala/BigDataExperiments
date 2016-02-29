name := "Pagerank-GraphX"

version := "1.0"

scalaVersion := "2.10.6"
exportJars := true
mainClass in(Compile, run) := Some("PageRank")
mainClass in(Compile, packageBin) := Some("PageRank")

//mainClass in assembly := Some("PageRank")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.0"
libraryDependencies += "org.jsoup" % "jsoup" % "1.7.2"
/*
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".conf" => MergeStrategy.concat
  case PathList(ps@_*) if ps.last endsWith ".class" => MergeStrategy.first
  case "application.conf" => MergeStrategy.first
  case "unwanted.txt" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
*/