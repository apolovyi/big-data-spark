name := "testNew"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

resolvers ++= Seq("apache-snapshots" at "http://repository.apache.org/.apache.org/snapshots/")

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "edu.umd" % "cloud9" % "1.4.17"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.5.0"
//libraryDependencies += "databricks" % "spark-corenlp" % "0.4.0-spark2.4-scala2.11"




