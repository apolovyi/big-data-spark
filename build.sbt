name := "big-data-spark"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

resolvers ++= Seq("apache-snapshots" at "http://repository.apache.org/.apache.org/snapshots/")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "edu.umd" % "cloud9" % "1.4.17",
  "com.databricks" %% "spark-xml" % "0.5.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" classifier "models",
  "org.elasticsearch" % "elasticsearch-hadoop" % "7.2.0"
)
