val typelevelV = "0.8.7"

addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.17.1")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.3.1")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.9.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
addSbtPlugin("org.typelevel" % "sbt-typelevel" % typelevelV)
addSbtPlugin("org.typelevel" % "sbt-typelevel-site" % typelevelV)
addDependencyTreePlugin
