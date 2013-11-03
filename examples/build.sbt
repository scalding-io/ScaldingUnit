organization := "com.pragmasoft"

name := "scalding-unit-examples"

version := "0.1"

scalaVersion := "2.10.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature")

libraryDependencies ++= Common.deps ++ Seq(
    "com.github.nscala-time" %% "nscala-time" % "0.6.0"
)

