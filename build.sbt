name := "DirectKafkaStreamEnhancements"

version := "1.0"

scalaVersion := "2.11.6"

retrieveManaged := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0",
  "org.apache.spark" %% "spark-streaming" % "1.4.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.0")

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += Resolver.sonatypeRepo("public")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += Classpaths.sbtPluginReleases

