organization := "com.pragmasoft"

name := "scalding-unit"

version := "0.1"

scalaVersion := "2.10.2"

resolvers += "mvn-repository" at "http://mvnrepository.com"

resolvers += "mvn-twitter" at "http://maven.twttr.com" 

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature")

libraryDependencies ++= Common.deps

