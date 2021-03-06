ThisBuild / version := "3.1.0"
ThisBuild / organization := "org.auth.csd.datalab"
ThisBuild / scalaVersion := "2.12.12"

val sparkVersion = "3.0.1"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
assembly / mainClass := Some("outlier_detection.Outlier_detection")
// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)


lazy val root = (project in file(".")).
  settings(
    name := "PROUD"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-mllib-local" % sparkVersion,
)

libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"

libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion
// For RDD
libraryDependencies += "com.github.fsanaulla" %% "chronicler-spark-rdd" % "0.4.1"
// For Dataset
libraryDependencies += "com.github.fsanaulla" %% "chronicler-spark-ds" % "0.4.1"
// For Structured Streaming
libraryDependencies += "com.github.fsanaulla" %% "chronicler-spark-structured-streaming" % "0.4.1"
// For DStream
libraryDependencies += "com.github.fsanaulla" %% "chronicler-spark-streaming" % "0.4.1"

libraryDependencies += "com.github.fsanaulla" %% "chronicler-spark-streaming" % "0.4.1"

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

Compile / run / fork := true
Global / cancelable := true
