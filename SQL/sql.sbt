name := "SQLApp"

version := "1.0"

scalaVersion := "2.12.10"


libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"

resolvers +="Local Maven Repository" at "file:///home/limin/.m2/repository"

resolvers +=Resolver.mavenLocal

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
