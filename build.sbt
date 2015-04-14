lazy val root = (project in file(".")).
  settings(
    name := "inadco-csjb",
    version := "1.0",
    scalaVersion := "2.10.5",
    libraryDependencies += "com.typesafe" % "config" % "1.2.1",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha2",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.2.1",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.2.1",
    libraryDependencies += "org.spark-project.hive" % "hive-cli" % "0.13.1",
    libraryDependencies += "org.spark-project.hive" % "hive-jdbc" % "0.13.1",
    libraryDependencies += "org.spark-project.hive" % "hive-beeline" % "0.13.1",
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.9",
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"
  )


//--------------- customize settings for sbt-assembly -------------
//output path
target in assembly := file(distDir)

//merge
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.last
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  
  case x if x.endsWith(".class") => MergeStrategy.last
  case x if x.endsWith(".properties") => MergeStrategy.last
  case x if x.contains("/resources/") => MergeStrategy.last
  case x if x.contains(".xml") => MergeStrategy.last 
  case x if x.contains(".thrift") => MergeStrategy.last
  case x if x.contains(".dtd") => MergeStrategy.last  
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

val distDir = "target/dist"
//---------- copy assets to target/dis--------------
val copyAssets = taskKey[Unit]("Copy additional assets to target")

copyAssets := {  
	//copy file dir
  val binDir = baseDirectory.value / "bin" 
  val binDirTarget = file(distDir + "/bin")
  
  sbt.IO.copyDirectory(binDir, binDirTarget, overwrite = true)
  Process("chmod -R +x target/dist/bin/*sh").!  
    
  println("Copied files from " + binDir.getAbsolutePath + " to " + binDirTarget.getAbsolutePath);
  
  //copy config dir
  val confDir = baseDirectory.value / "config" 
  val confDirTarget = file(distDir + "/config")
  sbt.IO.copyDirectory(confDir, confDirTarget, overwrite = true)
    
  println("Copied files from " + confDir.getAbsolutePath + " to " + confDirTarget.getAbsolutePath);
}

val zipDist = taskKey[Unit]("Zip the dist dir into a tar ball")

zipDist := {
  val distSrcFile = file(distDir)
  val distTargetFile = baseDirectory.value / "target" / (Keys.name.value + "-" + Keys.version.value + "-SNAPSHOT-bin" + ".tar.gz")
  
  IO.delete(distTargetFile)
  
  //this doesn't work for gzip dir
  //IO.gzip(distSrcFile, distTargetFile)
  
  //run external command line using Process
  
  val cmd = "tar -C target/dist -zcvf " + distTargetFile.getAbsolutePath + " ."

  Process(cmd).!
  
  println("Distribution tar ball file created at " + distTargetFile.getAbsolutePath)

}


