ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.9"

lazy val root = (project in file("."))
  .settings(
    name := "AkkaExperiments"
  )
val AkkaVersion = "2.6.20"
val catsVersion = "2.6.0"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % catsVersion,

  "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.14.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.14" % "test"
)
