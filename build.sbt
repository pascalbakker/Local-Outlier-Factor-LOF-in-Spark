name := "finalProject"
version := "1.0"
scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "3.0.1",
	"org.apache.spark" %% "spark-sql" % "3.0.1",
	"org.apache.spark" %% "spark-mllib" % "3.0.1",
	"org.scalanlp" %% "breeze" % "1.1",
	"org.scalanlp" %% "breeze-natives" % "1.1",
	"org.scalanlp" %% "breeze-viz" % "1.1"
)
