name := "cse8803_project_template"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.10"              % "1.2.0" % "provided",
  "org.apache.spark"  % "spark-mllib_2.10"             % "1.2.0",
  "com.databricks"    % "spark-csv_2.10"               % "0.1",
  "com.chuusai"       % "shapeless_2.10.4"             % "2.0.0",
  "org.apache.spark"  % "spark-graphx_2.10"            % "1.2.1",
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  "org.apache.commons" % "commons-dbcp2" % "2.0.1"
)

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  "org.apache.commons" % "commons-dbcp2" % "2.0.1"
)
