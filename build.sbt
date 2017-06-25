name := "Simple Project"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

resolvers += "justwrote" at "http://repo.justwrote.it/releases/"


val SparkVersion = "2.1.0"
val SparkCassandraVersion = "2.0.2"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies ++= Seq(
  ("org.apache.spark" %%  "spark-core"  % SparkVersion % "provided").
    exclude("org.eclipse.jetty.orbit", "javax.transaction").
    exclude("org.eclipse.jetty.orbit", "javax.mail").
    exclude("org.eclipse.jetty.orbit", "javax.activation").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")
)

libraryDependencies ++= Seq(
  "com.datastax.spark"  %%  "spark-cassandra-connector" % SparkCassandraVersion withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % SparkVersion
)

libraryDependencies += "com.github.javafaker" % "javafaker" % "0.13"
