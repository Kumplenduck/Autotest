val sparkVersion = "2.4.2"

lazy val root = (project in file(".")).
  settings(

    inThisBuild(List(
      organization := "TSC",
      scalaVersion := "2.11.12",
      version := "0.1.0-SNAPSHOT"
    )),
    name := "autotest",
    assemblyOutputPath in assembly := file("lib/autotest.jar"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-yarn" % sparkVersion % Provided
    )
  )
