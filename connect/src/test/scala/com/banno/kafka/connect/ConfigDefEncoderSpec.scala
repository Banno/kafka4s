package com.banno.kafka.connect

import org.scalatest._
import org.apache.kafka.common.config.ConfigDef
import scala.collection.JavaConverters._

case class MissingDocs(s: String)

class ConfigDefEncoderSpec extends FlatSpec with Matchers {

  "ConfigDefEncoder" should "Encode TestConnectorConfigs as ConfigDef" in {
    val cd = ConfigDefEncoder[TestConnectorConfigs].encode
    cd.configKeys should have size 6

    val c = cd.configKeys.values.asScala.find(_.name == "c").get
    c.`type` should ===(ConfigDef.Type.STRING)
    c.importance should ===(ConfigDef.Importance.MEDIUM)
    c.defaultValue should ===("1 second")
    c.documentation should ===("c docs")

    val e = cd.configKeys.values.asScala.find(_.name == "e").get
    e.defaultValue should be(null)
  }

  it should "require Documentation on every field" in {
    "ConfigDefEncoder[MissingDocs]" shouldNot compile
  }
}
