import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

object AkkaClusterTestappBuild extends Build {

  val akkaVersion = "2.3-20131122-230959"

  lazy val akkaSampleCluster = Project(
    id = "akka-sample-cluster-scala",
    base = file("."),
    settings = Project.defaultSettings ++ SbtMultiJvm.multiJvmSettings ++ Seq(
      name := "akka-sample-cluster-scala",
      version := "1.0",
      scalaVersion := "2.10.2",
      scalacOptions in Compile ++= Seq("-encoding", "UTF-8", "-target:jvm-1.6", "-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
      javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6", "-Xlint:unchecked", "-Xlint:deprecation"),
      // this is only needed while we use timestamped snapshot version of akka
      resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/",      
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
        "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
        "org.scalatest" %% "scalatest" % "1.9.2-SNAP2" % "test",
        "org.fusesource" % "sigar" % "1.6.4"),
      javaOptions in run ++= Seq(
        "-Djava.library.path=./sigar",
        "-Xms128m", "-Xmx1024m"),
      Keys.fork in run := true,  
      mainClass in (Compile, run) := Some("sample.cluster.simple.SimpleClusterApp")
    )
  ) configs (MultiJvm)
}
