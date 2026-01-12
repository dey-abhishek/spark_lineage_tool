name := "scala-parser"

version := "0.1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "com.github.scopt" %% "scopt" % "4.1.0",
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5"
)

// Assembly settings for creating fat JAR
assembly / assemblyJarName := "scala-parser.jar"
assembly / mainClass := Some("lineage.ScalaASTParser")
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

