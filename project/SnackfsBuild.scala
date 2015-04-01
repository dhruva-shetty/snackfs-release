/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import sbt._
import sbt.Keys._

object SnackfsBuild extends Build {

  lazy val VERSION = "0.6.1-EA"

  lazy val CAS_VERSION = "2.1.0"

  lazy val THRIFT_VERSION = "0.9.1"

  lazy val TWITTER_UTIL_VERSION = "6.7.0"

  lazy val dist = TaskKey[Unit]("dist", "Generates project distribution")

  lazy val dependencies = Seq("org.apache.hadoop" % "hadoop-common" % "2.4.0" % "provided",
    "org.apache.cassandra" % "cassandra-thrift" % CAS_VERSION intransitive(),
    "org.apache.cassandra" % "cassandra-all" % CAS_VERSION intransitive(),
    "org.apache.thrift" % "libthrift" % THRIFT_VERSION exclude("org.slf4j", "slf4j-api") exclude("javax.servlet", "servlet-api"),
    "commons-pool" % "commons-pool" % "1.6",
    "com.twitter" % "util-logging" % TWITTER_UTIL_VERSION cross CrossVersion.binaryMapped {
      case "2.9.3" => "2.9.2"
      case "2.10.3" => "2.10"
      case x => x
    },
    "org.scalatest" %% "scalatest" % "1.9.1" % "it,test",
    "junit" % "junit" % "4.8.1" % "test",
    "org.apache.commons" % "commons-io" % "1.3.2" % "it,test",
    "com.novocode" % "junit-interface" % "0.10" % "it,test",
    "org.apache.commons" % "commons-lang3" % "3.1" % "it,test",
    "net.jpountz.lz4" % "lz4" % "1.2.0",
    "org.eclipse.jetty.orbit" % "javax.servlet" % "2.5.0.v201103041518",
    "org.antlr" % "antlr-runtime" % "3.4",
    "com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" % "1.3",
    "com.google.guava" % "guava" % "15.0",
    "com.github.stephenc.high-scale-lib" % "high-scale-lib" % "1.1.4",
    "com.clearspring.analytics" % "stream" % "2.5.2",
    "com.yammer.metrics" % "metrics-core" % "2.2.0",
    "org.yaml" % "snakeyaml" % "1.12",
    "net.sf.supercsv" % "super-csv" % "2.1.0",
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.1",
    "com.github.jbellis" % "jamm" % "0.2.6",
    "com.typesafe.akka" %% "akka-actor" % "2.3.2",
    "com.typesafe.akka" %% "akka-remote" % "2.3.2",
    "com.google.protobuf" % "protobuf-java" % "2.5.0"
  )

  lazy val snackSettings = Project.defaultSettings ++ Seq(
    name := "snackfs",

    organization := "com.tuplejump",

    version := VERSION,

    crossScalaVersions := Seq("2.9.3", "2.10.3"),
    
    scalaVersion := "2.10.3",

    parallelExecution in Test := false,

    retrieveManaged := true,

    libraryDependencies ++= dependencies,

    parallelExecution in Test := false,

    publishArtifact in Test := false,

    pomIncludeRepository := {
      _ => false
    },

    publishMavenStyle := true,

    retrieveManaged := true,

    publishTo <<= version {
      (v: String) =>
        val nexus = "https://oss.sonatype.org/"
        if (v.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },

    licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),

    homepage := Some(url("https://www.blackrock.com")),

    organizationName := "BlackRock",

    organizationHomepage := Some(url("http://www.tuplejump.com")),
    
    resolvers += "Maven Central" at "http://central.maven.org/maven2/",
    
    resolvers += "Maven Repository" at "http://maven-repository.com/artifact/",
    
    resolvers += "Akka Repository" at "http://repo.akka.io/releases/",
    
    resolvers += "Spray Repository" at " http://repo.spray.cc/",
    
    excludeFilter in unmanagedResources := HiddenFileFilter || "core-site.xml" || "cassandra.yaml"
  )

  lazy val snackfs = Project(
    id = "snackfs",
    base = file("."),
    settings = snackSettings ++ Seq(distTask)
  ).configs(IntegrationTest).settings(Defaults.itSettings: _*)

  def distTask = dist in Compile <<= (packageBin in Compile, scalaVersion in Compile, version in Compile, streams) map {
    (f: File, sv: String, v: String, s) =>
      val userHome = System.getProperty("user.home")
      val ivyHome = userHome + "/.ivy2/cache/" //should be updated to point to ivy cache if its not in home directory

      val destination = "target/SnackFS_%s-%s/".format(sv, v)
      val lib = destination + "lib/"
      val bin = destination + "bin/"
      val conf = destination + "conf/"

      def twitterUtil =
        sv match {
          case "2.9.3" => Seq("util-core_2.9.2-" + TWITTER_UTIL_VERSION + ".jar", "util-logging_2.9.2-" + TWITTER_UTIL_VERSION + ".jar")
          case "2.10.3" => Seq("util-core_2.10-" + TWITTER_UTIL_VERSION + ".jar", "util-logging_2.10-" + TWITTER_UTIL_VERSION + ".jar")
          case x => Seq("util-core_2.10-" + TWITTER_UTIL_VERSION + ".jar", "util-logging_2.10-" + TWITTER_UTIL_VERSION + ".jar")
        }

      IO.copyFile(f, new File(lib + f.getName))

      /*Dependencies*/
      IO.copyFile(new File(ivyHome + "org.scala-lang/scala-library/jars/scala-library-" + sv + ".jar"),
        new File(lib + "scala-library-" + sv + ".jar"))

      val jars = getLibraries(sv)
      jars.foreach(j => {
        val jarFile = new File(j)
        IO.copyFile(jarFile, new File(lib + jarFile.getName))
        println(jarFile.getName)
      })

      /*script and configuration */
      val shellBin: sbt.File = new File(bin + "snackfs")
      IO.copyFile(new File("src/main/scripts/snackfs"), shellBin)
      shellBin.setExecutable(true, false)
      
      val serverBin: sbt.File = new File(bin + "snackfs-server")
      IO.copyFile(new File("src/main/scripts/snackfs-server"), serverBin)
      serverBin.setExecutable(true, false)
      
      IO.copyFile(new File("src/main/resources/core-site.xml"), new File(conf + "core-site.xml"))
      IO.copyFile(new File("src/main/resources/cassandra.yaml"), new File(conf + "cassandra.yaml"))

      val jarFiles = IO.listFiles(new File(lib))
      val configFiles = IO.listFiles(new File(conf))
      val scriptFiles = IO.listFiles(new File(bin))
      val allFiles = jarFiles ++ configFiles ++ scriptFiles
      val fileSeq = for (f <- allFiles) yield (f, f.getPath)

      val basedir: sbt.File = new File("target")
      val distdir: sbt.File = new File("target/snackfs_%s-%s".format(sv, v))
      val zipFile: sbt.File = new File("target/snackfs_%s-%s.zip".format(sv, v))
      def entries(f: File):List[File] = f :: (if (f.isDirectory) IO.listFiles(f).toList.flatMap(entries(_)) else Nil)
      IO.zip(entries(distdir).map(d => (d, d.getAbsolutePath.substring(basedir.getAbsolutePath.length + 1))), zipFile)
      IO.delete(distdir)
      s.log.info("SnackFS Distribution created at %s".format(zipFile.getAbsolutePath))
  }

  def getLibraries(sv: String): List[String] = {
    val jarSource = "lib_managed/jars/"
    val jarBundle = "lib_managed/bundles/"

    val cassandra = jarSource + "org.apache.cassandra/"
    val cassandraRelated = List(cassandra + "cassandra-all/cassandra-all-" + CAS_VERSION + ".jar",
      cassandra + "cassandra-thrift/cassandra-thrift-" + CAS_VERSION + ".jar",
      jarSource + "org.apache.thrift/libthrift/libthrift-0.9.1.jar",
      jarSource + "commons-pool/commons-pool/commons-pool-1.6.jar",
      jarSource + "net.jpountz.lz4/lz4/lz4-1.2.0.jar",
      jarSource + "com.github.jbellis/jamm/jamm-0.2.6.jar",
      jarSource + "org.antlr/antlr-runtime/antlr-runtime-3.4.jar",
      jarSource + "com.googlecode.concurrentlinkedhashmap/concurrentlinkedhashmap-lru/concurrentlinkedhashmap-lru-1.3.jar",
      jarSource + "org.apache.commons/commons-lang3/commons-lang3-3.1.jar",
      jarSource + "com.github.stephenc.high-scale-lib/high-scale-lib/high-scale-lib-1.1.4.jar",
      jarSource + "com.yammer.metrics/metrics-core/metrics-core-2.2.0.jar",
      jarSource + "com.clearspring.analytics/stream/stream-2.5.2.jar"
    )

    val hadoopRelated = List(jarSource + "org.apache.hadoop/hadoop-common/hadoop-common-2.4.0.jar",
      jarSource + "org.apache.hadoop/hadoop-auth/hadoop-auth-2.4.0.jar",
      jarSource + "commons-cli/commons-cli/commons-cli-1.2.jar",
      jarSource + "commons-configuration/commons-configuration/commons-configuration-1.6.jar",
      jarSource + "commons-lang/commons-lang/commons-lang-2.6.jar",
      jarSource + "commons-logging/commons-logging/commons-logging-1.1.3.jar"
    )

    val jackson = jarSource + "org.codehaus.jackson/"
    val log4j = "lib_managed/bundles/log4j/log4j/"

    val otherHadoopDeps = List(jackson + "jackson-core-asl/jackson-core-asl-1.8.8.jar",
      jackson + "jackson-mapper-asl/jackson-mapper-asl-1.8.8.jar",
      log4j + "log4j-1.2.17.jar",
      jarSource + "org.slf4j/slf4j-log4j12/slf4j-log4j12-1.7.5.jar",
      jarSource + "org.slf4j/slf4j-api/slf4j-api-1.7.5.jar"
    )

    val logger = jarSource + "com.twitter/"

    val loggingRelated =
      sv match {
        case "2.9.3" =>
          List(logger + "util-app_2.9.2/util-app_2.9.2-" + TWITTER_UTIL_VERSION + ".jar",
               logger + "util-core_2.9.2/util-core_2.9.2-" + TWITTER_UTIL_VERSION + ".jar",
               logger + "util-logging_2.9.2/util-logging_2.9.2-" + TWITTER_UTIL_VERSION + ".jar")

        case "2.10.3" =>
          List(logger + "util-app_2.10/util-app_2.10-" + TWITTER_UTIL_VERSION + ".jar",
               logger + "util-core_2.10/util-core_2.10-" + TWITTER_UTIL_VERSION + ".jar",
               logger + "util-logging_2.10/util-logging_2.10-" + TWITTER_UTIL_VERSION + ".jar")

        case x =>
          List(logger + "util-app_2.10/util-app_2.10-" + TWITTER_UTIL_VERSION + ".jar",
               logger + "util-core_2.10/util-core_2.10-" + TWITTER_UTIL_VERSION + ".jar",
               logger + "util-logging_2.10/util-logging_2.10-" + TWITTER_UTIL_VERSION + ".jar")
      }
    
    val otherJars = List(jarBundle + "com.google.guava/guava/guava-16.0.1.jar", 
                         jarSource + "commons-collections/commons-collections/commons-collections-3.2.1.jar",
                         jarBundle + "com.datastax.cassandra/cassandra-driver-core/cassandra-driver-core-2.1.1.jar",
                         jarBundle + "io.netty/netty/netty-3.9.0.Final.jar",
                         jarSource + "net.sf.supercsv/super-csv/super-csv-2.1.0.jar",
                         jarBundle + "org.yaml/snakeyaml/snakeyaml-1.12.jar",
                         jarSource + "commons-cli/commons-cli/commons-cli-1.2.jar",
                         jarSource + "com.typesafe.akka/akka-actor_2.10/akka-actor_2.10-2.3.2.jar",
                         jarSource + "com.typesafe.akka/akka-remote_2.10/akka-remote_2.10-2.3.2.jar",
                         jarBundle + "com.google.protobuf/protobuf-java/protobuf-java-2.5.0.jar",
                         jarBundle + "com.typesafe/config/config-1.2.0.jar")

    val requiredJars = cassandraRelated ++ hadoopRelated ++ otherHadoopDeps ++ loggingRelated ++ otherJars
    requiredJars
  }
}
