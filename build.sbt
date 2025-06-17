import laika.helium.Helium
import laika.helium.config.HeliumIcon
import laika.helium.config.IconLink
import org.typelevel.sbt.site.GenericSiteSettings
import xerial.sbt.Sonatype.sonatypeCentralHost

ThisBuild / scalaVersion := "2.13.16"
ThisBuild / crossScalaVersions := List(scalaVersion.value)
ThisBuild / tlBaseVersion := "6.3"
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("11"))
ThisBuild / githubWorkflowTargetBranches := Seq("*", "series/*")
ThisBuild / githubWorkflowBuildPreamble := Seq(
  WorkflowStep.Run(
    id = Some("start-docker-compose"),
    name = Some("Start docker-compose"),
    commands = List("docker compose up -d"),
  )
)

ThisBuild / sonatypeCredentialHost := sonatypeCentralHost

Global / onChangedBuildSource := ReloadOnSourceChanges

val V = new {
  val avro = "1.12.0"
  val avro4s = "3.1.0"
  val betterMonadicFor = "0.3.1"
  val cats = "2.13.0"
  val catsEffect = "3.6.1"
  val commonsCompress = "1.27.1"
  val confluent = "7.9.2"
  val curator = "5.8.0"
  val disciplineMunit = "2.0.0"
  val epimetheus = "0.5.0"
  val fs2 = "3.12.0"
  val guava = "32.1.3-jre"
  val junit = "4.13"
  val kafka = s"$confluent-ccs"
  val kindProjector = "0.13.3"
  val log4cats = "2.7.1"
  val logback = "1.5.18"
  val natchez = "0.3.8"
  val scalacheck = "1.18.1"
  val scalacheckEffect = "1.0.4"
  val scalacheckMagnolia = "0.6.0"
  val munit = "1.1.1"
  val munitScalacheck = "1.1.0"
  val munitCE3 = "2.1.0"
  val scalatest = "3.2.19"
  val scalatestPlus = "3.2.4.0"
  val snappy = "1.1.10.7"
  val vulcan = "1.12.0"
}

lazy val kafka4s = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, avro4s, vulcan, examples, site)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "kafka4s",
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core._
      import com.typesafe.tools.mima.core.ProblemFilters._
      Seq(
      )
    },
  )
  .settings(
    scalacOptions += "-Wnonunit-statement",
    testFrameworks += new TestFramework("munit.Framework"),
    libraryDependencies ++= Seq(
      "org.apache.curator" % "curator-test" % V.curator % Test,
      ("org.apache.kafka" %% "kafka" % V.kafka % Test).classifier("test"),
      ("org.apache.kafka" % "kafka-clients" % V.kafka % Test)
        .classifier("test"),
      ("org.apache.kafka" % "kafka-streams" % V.kafka % Test)
        .classifier("test"),
      ("org.apache.kafka" % "kafka-streams-test-utils" % V.kafka % Test),
      "ch.qos.logback" % "logback-classic" % V.logback % Test,
      "org.scalacheck" %% "scalacheck" % V.scalacheck % Test,
      "org.scalameta" %% "munit" % V.munit % Test,
      "org.scalameta" %% "munit-scalacheck" % V.munitScalacheck % Test,
      "org.typelevel" %% "scalacheck-effect-munit" % V.scalacheckEffect,
      "org.typelevel" %% "munit-cats-effect" % V.munitCE3 % Test,
      "org.typelevel" %% "cats-effect" % V.catsEffect,
      "org.tpolecat" %% "natchez-core" % V.natchez,
      "org.typelevel" %% "cats-laws" % V.cats % Test,
      "org.scalatest" %% "scalatest" % V.scalatest % Test,
      "org.typelevel" %% "discipline-munit" % V.disciplineMunit % Test,
    ),
  )

lazy val avro4s = project
  .in(file("avro4s"))
  .settings(commonSettings)
  .settings(
    name := "kafka4s-avro4s",
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core._
      import com.typesafe.tools.mima.core.ProblemFilters._
      Seq()
    },
    libraryDependencies ++= Seq(
      "com.sksamuel.avro4s" %% "avro4s-core" % V.avro4s,
      "com.github.chocpanda" %% "scalacheck-magnolia" % V.scalacheckMagnolia % Test,
      "org.scalatest" %% "scalatest" % V.scalatest % Test,
      "org.scalatestplus" %% "scalacheck-1-16" % "3.2.14.0" % Test,
    ),
  )
  .settings(
    scalacOptions += "-Wnonunit-statement",
    testFrameworks += new TestFramework("munit.Framework"),
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val vulcan = project
  .in(file("vulcan"))
  .settings(commonSettings)
  .settings(
    name := "kafka4s-vulcan",
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core._
      import com.typesafe.tools.mima.core.ProblemFilters._
      Seq()
    },
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "vulcan" % V.vulcan
    ),
  )
  .settings(
    scalacOptions += "-Wnonunit-statement",
    testFrameworks += new TestFramework("munit.Framework"),
  )
  .dependsOn(core)

lazy val examples = project
  .enablePlugins(NoPublishPlugin)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % V.logback % Runtime
    ),
    fork := true,
  )
  .disablePlugins(MimaPlugin)
  .dependsOn(core, avro4s)

lazy val site = project
  .in(file("site"))
  .settings(publish / skip := true)
  .enablePlugins(TypelevelSitePlugin)
  .enablePlugins(TypelevelUnidocPlugin)
  .dependsOn(core, avro4s)
  .settings {
    Seq(
      mdocIn := baseDirectory.value / "docs",
      tlSiteHelium := {
        GenericSiteSettings.defaults.value.site
          .metadata(
            title = Some("kafka4s"),
            description = Some("Functional programming with Kafka and Scala"),
            authors = List("Jack Henry & Associates, Inc.®"),
          )
          .site
          .topNavigationBar(
            homeLink = IconLink
              .external("https://banno.github.io/kafka4s", HeliumIcon.home)
          )
      },
    )
  }

lazy val commonSettings = Seq(
  resolvers += "confluent".at("https://packages.confluent.io/maven/"),
  addCompilerPlugin(
    ("org.typelevel" %% "kind-projector" % V.kindProjector)
      .cross(CrossVersion.full)
  ),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % V.betterMonadicFor),
  libraryDependencies ++= Seq(
    "co.fs2" %% "fs2-core" % V.fs2,
    "org.apache.kafka" % "kafka-clients" % V.kafka,
    "org.xerial.snappy" % "snappy-java" % V.snappy, // multiple CVE, doesn't work as Runtime
    "io.confluent" % "kafka-avro-serializer" % V.confluent,
    "org.apache.avro" % "avro" % V.avro, // CVE-2023-39410, didn't work as Runtime
    "org.apache.commons" % "commons-compress" % V.commonsCompress, // CVE-2023-42503, didn't work as Runtime
    "io.chrisdavenport" %% "epimetheus" % V.epimetheus,
    "org.typelevel" %% "log4cats-slf4j" % V.log4cats,
    // Upgrade vulnerable guava-30.1.1-jre from confluent-7.4.1.  This
    // should be a Runtime dependency, but it isn't shadowing right
    // unless it's Compile.
    "com.google.guava" % "guava" % V.guava,
  ),
  Test / sourceGenerators += (Test / avroScalaGenerate).taskValue,
  watchSources ++= ((Test / avroSourceDirectories).value ** "*.avdl").get,
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oS"),
  // Autogenerated source sometimes has unused warnings.  Works for Scala 2 only!
  Compile / scalacOptions += "-Wconf:src=target/.*:silent",
)

lazy val contributors = Seq(
  "amohrland" -> "Andrew Mohrland",
  "zcox" -> "Zach Cox",
  "kazark" -> "Keith Pinson",
)

inThisBuild(
  List(
    organization := "com.banno",
    developers := {
      for {
        (username, name) <- contributors
      } yield {
        Developer(username, name, "", url(s"http://github.com/$username"))
      }
    }.toList,
    scalacOptions ++= Seq(
      "-Xsource:3",
      "-Vimplicits",
      "-Vtype-diffs",
      "-language:postfixOps",
      "-Xlog-free-terms",
      "-Xlog-free-types",
    ),
    organizationName := "Jack Henry & Associates, Inc.®",
    startYear := Some(2019),
    licenses := Seq(License.Apache2),
    homepage := Some(url("https://github.com/banno/kafka4s")),
  )
)

addCommandAlias("fmt", "scalafmtSbt;scalafmtAll;")
addCommandAlias("fmtck", "scalafmtSbtCheck;scalafmtCheckAll;")
addCommandAlias("build", "Test / compile")
