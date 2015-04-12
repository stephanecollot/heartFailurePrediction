import AssemblyKeys._

assemblySettings

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