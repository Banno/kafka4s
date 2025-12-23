val typelevelV = "0.8.4"

addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.15.0")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.3.1")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.8.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.6")
addSbtPlugin("org.typelevel" % "sbt-typelevel" % typelevelV)
addSbtPlugin("org.typelevel" % "sbt-typelevel-site" % typelevelV)
addDependencyTreePlugin
