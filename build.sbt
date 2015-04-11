import AssemblyKeys._

assemblySettings

name := "cse8803_project_template"

version := "1.0"

scalaVersion := "2.10.4"

logLevel := Level.Info

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.10"              % "1.2.0" % "provided",
  "org.apache.spark"  % "spark-mllib_2.10"             % "1.2.0",
  "com.databricks"    % "spark-csv_2.10"               % "0.1",
  "com.chuusai"       % "shapeless_2.10.4"             % "2.0.0",
  "org.apache.spark"  % "spark-graphx_2.10"            % "1.2.1",
  "org.postgresql" % "postgresql" % "9.3-1103-jdbc41",
  "org.apache.commons" % "commons-dbcp2" % "2.0.1",
  "com.typesafe" % "config" % "1.2.1",
 "org.slf4j" % "slf4j-api" % "1.7.12",
    "org.slf4j" % "slf4j-simple" % "1.7.12",
    "spark.jobserver" % "job-server-api" % "0.5.0" % "provided"
)

val buildSettings = Defaults.defaultSettings ++ Seq(
  javaOptions += "-Xmx2G -XX:MaxPermSize=512m"
)

mainClass in assembly := Some("edu.gatech.cse8803.main.Main")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
  case "about.html"  => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
}
