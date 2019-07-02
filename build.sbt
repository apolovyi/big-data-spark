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
  "org.elasticsearch.client" % "transport" % "7.2.0",
  "org.elasticsearch.client" % "elasticsearch-rest-client" % "7.2.0",
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.2.0",
  "org.elasticsearch" % "elasticsearch" % "7.2.0",
  "org.elasticsearch" %% "elasticsearch-spark" % "5.0.0-alpha4"
)


//libraryDependencies += "databricks" % "spark-corenlp" % "0.4.0-spark2.4-scala2.11"
//libraryDependencies += "databricks" % "spark-corenlp" % "0.4.0-spark2.4-scala2.11"

//libraryDependencies += "databricks" % "spark-corenlp" % "0.2.0-s_2.11"

