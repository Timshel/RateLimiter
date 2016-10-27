
organization := "timshel"
version := "0.0.1"
scalaVersion := "2.11.8"

resolvers := Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  DefaultMavenRepository
)

scalacOptions := Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Xlint:-missing-interpolator", // Waiting on Play 2.5
  "-Yinline-warnings",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-unused-import",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture"
)

fork := true
parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.4.11",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.8" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)
