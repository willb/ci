name := "rhci"

organization := "com.redhat.et"

version := "0.0.1"

scalaVersion := "2.11.8"

val SPARK_VERSION = "2.1.0"
val SCALA_VERSION = "2.11.8"

resolvers += "Will's bintray" at "https://dl.bintray.com/willb/maven/"

def commonSettings = Seq(
  libraryDependencies ++= Seq(
    "com.github.scopt" %% "scopt" % "3.5.0",
    "org.apache.spark" %% "spark-core" % SPARK_VERSION,
    "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
    "org.apache.spark" %% "spark-mllib" % SPARK_VERSION,
    "org.scala-lang" % "scala-reflect" % SCALA_VERSION,
    "com.redhat.et" %% "silex" % "0.1.1",
    "org.elasticsearch" %% "elasticsearch-spark-20" % "5.1.1"
  )
)

seq(commonSettings:_*)

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/root-doc.txt")

(dependencyClasspath in Test) <<= (dependencyClasspath in Test).map(
  _.filterNot(_.data.name.contains("slf4j-log4j12"))
)

lazy val rhci = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("com.redhat.et.rhci.Main"),
    assemblyMergeStrategy in assembly := { 
      case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
      case "log4j.properties"                                  => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      case "reference.conf"                                    => MergeStrategy.concat
      case _                                                   => MergeStrategy.first
    }
  )
