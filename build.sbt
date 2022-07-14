ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

val circeVersion = "0.14.1"
val sparkVersion = "3.2.1"
val esVersion    = "8.2.3"

lazy val root = (project in file("."))
  .settings(
    name := "spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.elasticsearch" %% "elasticsearch-spark-30" % esVersion
    )
  )

