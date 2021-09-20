val sparkVersion = "3.0.1"

lazy val root = project
    .withId("lshis")
    .in(file("."))
    .settings(
        name          := "lshis",
        organization  := "ubu.admirable",
        scalaVersion  := "2.12.10",
        version       := "0.0.1",
        licenses      := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
        resolvers     += "Spark Packages Repo" at "https://repos.spark-packages.org",
        libraryDependencies ++= Seq(
            "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
        )
    )
