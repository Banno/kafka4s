val typelevelV = "0.8.6"

addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.17.0")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.3.1")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.9.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.1")
addSbtPlugin("org.typelevel" % "sbt-typelevel" % typelevelV)
addSbtPlugin("org.typelevel" % "sbt-typelevel-site" % typelevelV)
addDependencyTreePlugin
