name := "cse8803_hw3_template"

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
  "org.apache.spark"  % "spark-graphx_2.10"            % "1.2.1"
)
