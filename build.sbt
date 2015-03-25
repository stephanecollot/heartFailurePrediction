name := "cse8803_hw3_template"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0",
  "org.apache.spark" %% "spark-mllib" % "1.3.0",
  "com.databricks"    % "spark-csv_2.10" % "1.0.0",
  "com.chuusai"       % "shapeless_2.10.4"             % "2.0.0",
  "org.apache.spark" %% "spark-graphx" % "1.3.0"
)
