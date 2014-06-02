name := "scala-accumulo"

organization := "com.tetra"

version := "1.0"

organizationName := "Tetra Concepts LLC"

organizationHomepage := Some(url("http://tetraconcepts.com"))

startYear := Some(2014)

description := "A light wrapper to provide a fluent api for reading data from Apache Accumulo in Scala."

licenses += "Apachev2" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "17.0",
  "com.google.code.findbugs" % "jsr305" % "2.0.1",
  "commons-collections" % "commons-collections" % "3.2.1",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.accumulo" % "accumulo-core" % "[1.5,)" exclude("com.google.guava", "guava"),
  "org.apache.hadoop" % "hadoop-core" % "0.20.2" % "provided" 
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.1.7" % "test",
  "org.easymock" % "easymock" % "3.2" % "test"
)
