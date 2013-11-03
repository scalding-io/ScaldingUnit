organization := "com.pragmasoft"

name := "scalding-unit-examples"

version := "0.1"

scalaVersion := "2.10.2"

resolvers ++= Seq(
	"mvn-repository" at "http://mvnrepository.com"
)

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature")

libraryDependencies ++= Common.deps

