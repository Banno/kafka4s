Global / onChangedBuildSource := ReloadOnSourceChanges

val V = new {
  val scala_2_13 = "2.13.4"
  val scala_2_12 = "2.12.12"
  val avro4s = "3.1.0"
  val betterMonadicFor = "0.3.1"
  val cats = "2.4.2"
  val confluent = "6.0.1"
  val curator = "5.1.0"
  val discipline = "2.0.1"
  val fs2 = "2.5.0"
  val github4s = "0.28.2"
  val junit = "4.13"
  val kafka = "2.7.0"
  val kindProjector = "0.11.3"
  val log4cats = "1.1.1"
  val log4j = "1.7.30"
  val logback = "1.2.3"
  val scalacheck = "1.15.3"
  val scalacheckMagnolia = "0.6.0"
  val scalatest = "3.2.5"
  val scalatestPlus = "3.2.3.0"
  val simpleClient = "0.9.0"
}

lazy val kafka4s = project
  .in(file("."))
  .settings(scalaVersion := V.scala_2_12)
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, examples, site)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "kafka4s",
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core._
      import com.typesafe.tools.mima.core.ProblemFilters._
      Seq()
    },
    scalacOptions --= Seq(
      "-Wunused:imports",
      "-Ywarn-unused:imports",
    ),
  )
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.2",
      "org.apache.curator" % "curator-test" % V.curator % "test",
      ("org.apache.kafka" %% "kafka" % V.kafka % "test").classifier("test"),
      ("org.apache.kafka" % "kafka-clients" % V.kafka % "test").classifier("test"),
      ("org.apache.kafka" % "kafka-streams" % V.kafka % "test").classifier("test"),
      ("org.apache.kafka" % "kafka-streams-test-utils" % V.kafka % "test"),
      "ch.qos.logback" % "logback-classic" % V.logback % "test",
      "org.slf4j" % "log4j-over-slf4j" % V.log4j % "test",
      "org.scalacheck" %% "scalacheck" % V.scalacheck % "test",
      "org.scalatest" %% "scalatest" % V.scalatest % "test",
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.3.0" % Test,
      "com.github.chocpanda" %% "scalacheck-magnolia" % V.scalacheckMagnolia % "test",
      "org.typelevel" %% "cats-laws" % V.cats % "test",
      "org.typelevel" %% "discipline-scalatest" % V.discipline % "test",
    )
  )

lazy val examples = project
  .enablePlugins(NoPublishPlugin)
  .settings(commonSettings)
  .settings(libraryDependencies += "dev.zio" %% "zio-interop-cats" % "2.2.0.1")
  .disablePlugins(MimaPlugin)
  .dependsOn(core)

lazy val site = project
  .in(file("site"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(MicrositesPlugin)
  .enablePlugins(MdocPlugin)
  .enablePlugins(NoPublishPlugin)
  .settings(commonSettings)
  .dependsOn(core)
  .settings {
    import microsites._
    Seq(
      micrositeName := "kafka4s",
      micrositeDescription := "Functional programming with Kafka and Scala",
      micrositeAuthor := "Jack Henry & Associates, Inc.®",
      micrositeGithubOwner := "Banno",
      micrositeGithubRepo := "kafka4s",
      micrositeTwitter := "@kafka4s",
      micrositeBaseUrl := "/kafka4s",
      micrositeDocumentationUrl := "/kafka4s/docs",
      micrositeFooterText := None,
      micrositeHighlightTheme := "atom-one-light",
      micrositePalette := Map(
        "brand-primary" -> "#3e5b95",
        "brand-secondary" -> "#294066",
        "brand-tertiary" -> "#2d5799",
        "gray-dark" -> "#49494B",
        "gray" -> "#7B7B7E",
        "gray-light" -> "#E5E5E6",
        "gray-lighter" -> "#F4F3F4",
        "white-color" -> "#FFFFFF",
      ),
      libraryDependencies += "com.47deg" %% "github4s" % V.github4s,
      micrositePushSiteWith := GitHub4s,
      micrositeGithubToken := sys.env.get("GITHUB_TOKEN"),
      micrositeExtraMdFiles := Map(
        file("CHANGELOG.md") -> ExtraMdFileConfig(
          "changelog.md",
          "page",
          Map("title" -> "changelog", "section" -> "changelog", "position" -> "100"),
        ),
        file("CODE_OF_CONDUCT.md") -> ExtraMdFileConfig(
          "code-of-conduct.md",
          "page",
          Map("title" -> "code of conduct", "section" -> "code of conduct", "position" -> "101"),
        ),
        file("LICENSE") -> ExtraMdFileConfig(
          "license.md",
          "page",
          Map("title" -> "license", "section" -> "license", "position" -> "102"),
        ),
      ),
    )
  }

lazy val commonSettings = Seq(
  scalaVersion := V.scala_2_12,
  crossScalaVersions := Seq(scalaVersion.value, V.scala_2_13),
  resolvers += "confluent".at("https://packages.confluent.io/maven/"),
  addCompilerPlugin(
    ("org.typelevel" %% "kind-projector" % V.kindProjector).cross(CrossVersion.full),
  ),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % V.betterMonadicFor),
  libraryDependencies ++= Seq(
    "co.fs2" %% "fs2-core" % V.fs2,
    "org.apache.kafka" % "kafka-clients" % V.kafka,
    "io.confluent" % "kafka-avro-serializer" % V.confluent,
    "com.sksamuel.avro4s" %% "avro4s-core" % V.avro4s,
    "io.prometheus" % "simpleclient" % V.simpleClient,
    "io.chrisdavenport" %% "log4cats-slf4j" % V.log4cats,
  ),
  sourceGenerators in Test += (avroScalaGenerate in Test).taskValue,
  watchSources ++= ((avroSourceDirectories in Test).value ** "*.avdl").get,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oS"),
)

lazy val contributors = Seq(
  "amohrland" -> "Andrew Mohrland",
  "zcox" -> "Zach Cox",
)

inThisBuild(
  List(
    organization := "com.banno",
    developers := {
      for {
        (username, name) <- contributors
      } yield {
        Developer(username, name, "", url(s"http://github.com/$username"))
      },
    }.toList,
    scalacOptions ++= Seq(
      "-language:postfixOps",
      "-Xlog-free-terms",
      "-Xlog-free-types",
    ),
    pomIncludeRepository := { _ =>
      false
    },
    organizationName := "Jack Henry & Associates, Inc.®",
    startYear := Some(2019),
    licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage := Some(url("https://github.com/banno/kafka4s")),
  ),
)

addCommandAlias("fmt", "scalafmtSbt;scalafmtAll;")
addCommandAlias("fmtck", "scalafmtSbtCheck;scalafmtCheckAll;")
