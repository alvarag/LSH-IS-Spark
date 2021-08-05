name          := "lshis"
version       := "0.0.1"
organization  := "ubu.admirable"
licenses      := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scalaVersion  := "2.12.10"

//sbt-spark-package
spName          := "alvarag/lsh-is-bd"
sparkVersion    := "3.0.1"

sparkComponents += "mllib"

//include provided dependencies in sbt run task
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
