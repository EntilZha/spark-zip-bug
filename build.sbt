name := "graphx-bug"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.2.0",
  "org.apache.spark" % "spark-graphx_2.10" % "1.2.0"
)
