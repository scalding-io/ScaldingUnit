organization := "com.pragmasoft"

name := "scaldingunit"

version := "0.1"

scalaVersion := "2.10.2"

lazy val main = project.in(file("scalding-unit"))

lazy val examples = project.in(file("examples")).dependsOn(main)

lazy val root = project.in(file(".")).aggregate(main, examples)
