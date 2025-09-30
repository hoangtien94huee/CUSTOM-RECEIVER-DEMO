name := "spark-streaming-custom-receiver"
version := "0.1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.3.0" % "provided",
  "org.json4s" %% "json4s-native" % "4.0.5",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.3"
)

fork / run := true