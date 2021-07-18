name := "spark-currency-conversion"

version := "0.1"

scalaVersion := "2.12.14"

//idePackagePrefix := Some("org.example.transaction")


val testDependencies = Seq(
  "org.scalactic" %% "scalactic" % "3.2.9",
  "org.scalatest" %% "scalatest" % "3.2.9"
)

resolvers += Resolver.sonatypeRepo("releases")

assembly / assemblyOption ~= {
  _.withIncludeScala(true)
   .withIncludeDependency(true)
}

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.12" % "3.1.2" % "provided",
  "org.apache.spark" % "spark-catalyst_2.12" % "3.1.2" % "provided",

  "com.twitter" %% "scalding-args" % "0.17.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "com.typesafe" % "config" % "1.4.1",
) ++ testDependencies.map(_ % "test")

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("application.conf") => MergeStrategy.discard
  case "BUILD" => MergeStrategy.discard
  case fileName if fileName.toLowerCase == "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.last
}
