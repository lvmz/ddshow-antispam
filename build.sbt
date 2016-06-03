import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._

name := "ddshow-antispam"

version := "1.0"

scalaVersion := "2.10.4"

//libraryDependencies += "org.apache.spark" %% "spark-assembly" % "1.5.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-assembly" % "1.5.2"


libraryDependencies ++= jarTopack



lazy val jarTopack =   Seq(
  "com.google.guava" % "guava" % "11.0.2",
  "com.alibaba" % "fastjson" % "1.2.7",
  "org.apache.lucene" % "lucene-core" % "4.0.0",
  "org.wltea.analyzer" % "ik-analyzer" % "3.2.8",
  "com.alibaba" % "druid" % "1.0.11",
  "mysql" % "mysql-connector-java" % "5.1.18",
  "com.belerweb" % "pinyin4j" % "2.5.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.5.2"
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class")
  => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}


lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.youku",
  scalaVersion := "2.10.4"
)

javacOptions ++= Seq("-encoding", "UTF-8")

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings( libraryDependencies ++= jarTopack )














