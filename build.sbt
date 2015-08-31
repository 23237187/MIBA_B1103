name := "ZTE_Mobile_Integrated_Behavior_Analysis"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "joda-time"   % "joda-time"           % "2.7",
  "commons-codec"           % "commons-codec"  % "1.9",
  "org.clapper" % "grizzled-slf4j_2.10" % "1.0.2",
  "org.json4s"  % "json4s-native_2.10"  % "3.2.10",
  "org.json4s"  % "json4s-ext_2.10"     % "3.2.10",
  "org.apache.hadoop"       % "hadoop-common"  % "2.5.0"
    exclude("javax.servlet", "servlet-api"),
  "org.apache.hbase"        % "hbase-common"   % "0.98.5-hadoop2",
  "org.apache.hbase"        % "hbase-client"   % "0.98.5-hadoop2"
    exclude("org.apache.zookeeper", "zookeeper"),
  "org.apache.hbase"        % "hbase-server"   % "0.98.5-hadoop2"
    exclude("org.apache.hbase", "hbase-client")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("javax.servlet", "servlet-api")
    exclude("org.mortbay.jetty", "servlet-api-2.5")
    exclude("org.mortbay.jetty", "jsp-api-2.1")
    exclude("org.mortbay.jetty", "jsp-2.1"),
  "org.apache.zookeeper"    % "zookeeper"      % "3.4.6"
    exclude("org.slf4j", "slf4j-api")
    exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark"       %% "spark-core"     % "1.3.0" % "provided",
  "org.apache.spark"       %% "spark-sql"      % "1.3.0" % "provided",
  "org.elasticsearch"       % "elasticsearch"  % "1.4.4",
  "net.jodah"               % "typetools"      % "0.3.1",
  "com.google.code.gson"    % "gson"           % "2.2.4",
  "com.github.scopt"       %% "scopt"          % "3.2.0")

    