lazy val kafka4s = project.in(file("."))
  .settings(commonSettings, releaseSettings, skipOnPublishSettings)
  .aggregate(core, docs, examples)

lazy val core = project
  .settings(commonSettings, releaseSettings, mimaSettings)
  .settings(
    name := "kafka4s"
  )

lazy val docs = project
  .settings(commonSettings, skipOnPublishSettings, micrositeSettings)
  .dependsOn(core)
  .enablePlugins(MicrositesPlugin)
  .enablePlugins(TutPlugin)

lazy val examples = project
  .settings(commonSettings, skipOnPublishSettings)
  .dependsOn(core)
  .settings(
     libraryDependencies += "dev.zio" %% "zio-interop-cats" % "1.3.1.0-RC2"
  )

lazy val contributors = Seq(
  "amohrland" -> "Andrew Mohrland",
  "zcox"      -> "Zach Cox",
)

lazy val V = new {
  val scala_2_12 = "2.12.8"
  val cats = "1.6.1"
  val fs2 = "1.0.5"
  val kafka = "2.3.0"
  val confluent = "5.2.2"
  val avro4s = "1.8.4"
  val log4cats = "0.3.0"
  val scalacheckMagnolia = "0.0.2"
}

lazy val commonSettings = Seq(
  organization := "com.banno",
  scalaVersion := V.scala_2_12,
  crossScalaVersions := Seq(V.scala_2_12),

  publishArtifact in ThisBuild := true,

  cancelable in Global := true,

  scalacOptions ++= Seq(
    "-language:postfixOps",
    "-Xlog-free-terms",
    "-Xlog-free-types",
  ),

  organizationName := "Jack Henry & Associates, Inc.®",
  startYear := Some(2019),
  licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),

  resolvers += "confluent" at "http://packages.confluent.io/maven/",

  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3"),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.0"),
  libraryDependencies ++= Seq(
    "co.fs2"                       %% "fs2-core"                  % V.fs2,
    //TODO may no longer need logging excludes for kafka-clients, need to verify
    "org.apache.kafka"              % "kafka-clients"             % V.kafka exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j"),
    "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar"), // This explicit dependency is needed for confluent (see https://github.com/sbt/sbt/issues/3618#issuecomment-413257502)
    "io.confluent"                  % "kafka-avro-serializer"     % V.confluent exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeeper"),
    "com.sksamuel.avro4s"          %% "avro4s-core"               % V.avro4s,
    "io.prometheus"                 % "simpleclient"              % "0.6.0",
    "io.chrisdavenport"            %% "log4cats-slf4j"            % V.log4cats,
    "org.apache.curator"            % "curator-test"              % "4.2.0"          % "test",
    "org.apache.kafka"             %% "kafka"                     % V.kafka          % "test" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j"),
    "org.apache.kafka"             %% "kafka"                     % V.kafka          % "test" classifier "test" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j"),
    "org.apache.kafka"              % "kafka-clients"             % V.kafka          % "test" classifier "test" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j"),
    "io.confluent"                  % "kafka-schema-registry"     % V.confluent      % "test" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j"),
    "io.confluent"                  % "kafka-schema-registry"     % V.confluent      % "test" classifier "tests" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j"),
    "junit"                         % "junit"                     % "4.12"           % "test",
    "ch.qos.logback"                % "logback-classic"           % "1.2.3"          % "test",
    "org.slf4j"                     % "log4j-over-slf4j"          % "1.7.26"         % "test",
    "org.scalacheck"               %% "scalacheck"                % "1.14.0"         % "test",
    "org.scalatest"                %% "scalatest"                 % "3.0.8"          % "test",
    "com.mrdziuban"                %% "scalacheck-magnolia"       % V.scalacheckMagnolia % "test",
    "org.typelevel"                %% "cats-laws"                 % V.cats           % "test",
    "org.typelevel"                %% "discipline"                % "0.11.1"         % "test"
  ),
  sourceGenerators in Test += (avroScalaGenerate in Test).taskValue,
  watchSources ++= ((avroSourceDirectory in Test).value ** "*.avdl").get,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oS"),
)

lazy val releaseSettings = {
  import ReleaseTransformations._
  Seq(
    releaseCrossBuild := true,
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepCommandAndRemaining("+publishSigned"),
      setNextVersion,
      commitNextVersion,
      releaseStepCommand("sonatypeReleaseAll"),
      pushChanges
    ),
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    credentials ++= (
      for {
        username <- Option(System.getenv().get("SONATYPE_USERNAME"))
        password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
      } yield
        Credentials(
          "Sonatype Nexus Repository Manager",
          "oss.sonatype.org",
          username,
          password
        )
    ).toSeq,
    publishArtifact in Test := false,
    // releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/banno/kafka4s"),
        "git@github.com:banno/kafka4s.git"
      )
    ),
    homepage := Some(url("https://github.com/banno/kafka4s")),
    publishMavenStyle := true,
    pomIncludeRepository := { _ =>
      false
    },
    pomExtra := {
      <developers>
        {for ((username, name) <- contributors) yield
        <developer>
          <id>{username}</id>
          <name>{name}</name>
          <url>http://github.com/{username}</url>
        </developer>
        }
      </developers>
    }
  )
}

lazy val mimaSettings = {
  import sbtrelease.Version

  def semverBinCompatVersions(major: Int, minor: Int, patch: Int): Set[(Int, Int, Int)] = {
    val majorVersions: List[Int] =
      if (major == 0 && minor == 0) List.empty[Int] // If 0.0.x do not check MiMa
      else List(major)
    val minorVersions : List[Int] =
      if (major >= 1) Range(0, minor).inclusive.toList
      else List(minor)
    def patchVersions(currentMinVersion: Int): List[Int] =
      if (minor == 0 && patch == 0) List.empty[Int]
      else if (currentMinVersion != minor) List(0)
      else Range(0, patch - 1).inclusive.toList

    val versions = for {
      maj <- majorVersions
      min <- minorVersions
      pat <- patchVersions(min)
    } yield (maj, min, pat)
    versions.toSet
  }

  def mimaVersions(version: String): Set[String] = {
    Version(version) match {
      case Some(Version(major, Seq(minor, patch), _)) =>
        semverBinCompatVersions(major.toInt, minor.toInt, patch.toInt)
          .map{case (maj, min, pat) => maj.toString + "." + min.toString + "." + pat.toString}
      case _ =>
        Set.empty[String]
    }
  }
  // Safety Net For Exclusions
  lazy val excludedVersions: Set[String] = Set()

  // Safety Net for Inclusions
  lazy val extraVersions: Set[String] = Set()

  Seq(
    mimaFailOnProblem := mimaVersions(version.value).toList.headOption.isDefined,
    mimaPreviousArtifacts := (mimaVersions(version.value) ++ extraVersions)
      .filterNot(excludedVersions.contains(_))
      .map{v =>
        val moduleN = moduleName.value + "_" + scalaBinaryVersion.value.toString
        organization.value % moduleN % v
      },
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core._
      import com.typesafe.tools.mima.core.ProblemFilters._
      Seq()
    }
  )
}

lazy val micrositeSettings = {
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
      "white-color" -> "#FFFFFF"
    ),
    fork in tut := true,
    scalacOptions in Tut --= Seq(
      "-Xfatal-warnings",
      "-Ywarn-unused-import",
      "-Ywarn-numeric-widen",
      "-Ywarn-dead-code",
      "-Ywarn-unused:imports",
      "-Xlint:-missing-interpolator,_"
    ),
    libraryDependencies += "com.47deg" %% "github4s" % "0.20.1",
    micrositePushSiteWith := GitHub4s,
    micrositeGithubToken := sys.env.get("GITHUB_TOKEN"),
    micrositeExtraMdFiles := Map(
      file("CHANGELOG.md")        -> ExtraMdFileConfig("changelog.md", "page", Map("title" -> "changelog", "section" -> "changelog", "position" -> "100")),
      file("CODE_OF_CONDUCT.md")  -> ExtraMdFileConfig("code-of-conduct.md",   "page", Map("title" -> "code of conduct",   "section" -> "code of conduct",   "position" -> "101")),
      file("LICENSE")             -> ExtraMdFileConfig("license.md",   "page", Map("title" -> "license",   "section" -> "license",   "position" -> "102"))
    )
  )
}

lazy val skipOnPublishSettings = Seq(
  skip in publish := true,
  publish := (()),
  publishLocal := (()),
  publishArtifact := false,
  publishTo := None
)
