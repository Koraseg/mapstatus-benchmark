name := "MapBenchmark"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "com.github.axel22" %% "scalameter" % "0.5-M2",
  "org.roaringbitmap" % "RoaringBitmap" % "0.5.11"
)

testFrameworks += new TestFramework(
  "org.scalameter.ScalaMeterFramework")

logBuffered := false

parallelExecution in Test := false